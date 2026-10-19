/**
 * Testy E2E przepływu zamówień: orders -> consumer -> orders-processed / orders-dlq.
 *
 * Izolacja: każdy test używa własnych, unikalnych identyfikatorów (UUID)
 * i szuka na topicach wyjściowych tylko swoich wiadomości, a helpery czytają
 * topic nowym konsumentem z unikalnym groupId. Dzięki temu testy można
 * uruchamiać pojedynczo, w dowolnej kolejności i wielokrotnie na tym samym
 * środowisku.
 */
import { Producer } from "kafkajs";
import { randomUUID } from "crypto";
import {
  ORDERS_DLQ_TOPIC,
  ORDERS_PROCESSED_TOPIC,
  Order,
  anyMessage,
  byOrderId,
  byOrderIds,
  collectMessages,
  collectMessagesUntil,
  disconnectProducer,
  dlqByOriginalField,
  dlqByRaw,
  getProducer,
  isDlqParseError,
  isDlqValidationError,
  isOrder,
  makeTestOrder,
  sendOrder,
  sendRaw,
  sendRawBatch,
  waitForMessage,
  withKey,
} from "../src/kafkaHelpers";

const REQUIRED_FIELDS = ["orderId", "customer", "amount", "status"] as const;
const SLA_MS = 10_000;

describe("Przepływ przetwarzania zamówień przez Kafkę", () => {
  let producer: Producer;

  beforeAll(async () => {
    producer = await getProducer();

    // Rozgrzewka: upewniamy się, że konsument aplikacji działa i jest już
    // w grupie, zanim zaczniemy mierzyć czasy (ważne dla testu SLA, gdy
    // uruchamiamy go pojedynczo zaraz po starcie środowiska).
    const warmup = makeTestOrder();
    await sendOrder(producer, warmup);
    const processed = await waitForMessage(ORDERS_PROCESSED_TOPIC, byOrderId(warmup.orderId), 25);
    if (!processed) {
      throw new Error("Konsument nie przetworzył wiadomości rozgrzewkowej — czy serwis 'consumer' działa?");
    }
  }, 30_000);

  afterAll(async () => {
    await disconnectProducer();
  });

  /**
   * Sprawdza, że na 'orders-processed' NIE pojawiła się żadna wiadomość z kluczem `key`.
   *
   * Zamiast czekać pełny timeout, wysyłamy po sprawdzanej wiadomości poprawne
   * zamówienie-znacznik (sentinel) z tym samym kluczem — trafi na tę samą
   * partycję, więc konsument obsłuży je dopiero po sprawdzanej wiadomości.
   * Gdy znacznik pojawi się na wyjściu, wiemy, że wynik jest już ostateczny.
   */
  async function expectNotProcessed(key: string): Promise<void> {
    const sentinel = makeTestOrder();
    await sendRaw(producer, JSON.stringify(sentinel), key);

    // Sentinel ma ten sam klucz, ale jest sprawdzany jako pierwszy (stopFn),
    // więc do `matched` trafiają tylko wcześniejsze wiadomości z tym kluczem.
    const result = await collectMessagesUntil(
      ORDERS_PROCESSED_TOPIC,
      withKey(key, anyMessage),
      byOrderId(sentinel.orderId)
    );

    expect(result.stopFound).toBe(true);
    expect(result.matched).toEqual([]);
  }

  describe("poprawne zamówienia", () => {
    it("przetwarza poprawną wiadomość (happy path)", async () => {
      const order = makeTestOrder();

      await sendOrder(producer, order);
      const processed = await waitForMessage(ORDERS_PROCESSED_TOPIC, byOrderId(order.orderId));

      expect(processed).not.toBeNull();
      expect(processed?.status).toBe("PROCESSED");
    });

    it.each<[string, Partial<Order>]>([
      ["typowe dane", {}],
      ["polskie znaki w customer", { customer: "Zażółć Gęślą-Jaźń" }],
      ["amount równe 0", { amount: 0 }],
      ["amount z wieloma miejscami po przecinku", { amount: 0.1 + 0.2 }],
      ["duża kwota", { amount: 9_999_999.99 }],
    ])("zachowuje oryginalne dane w przetworzonej wiadomości (%s)", async (_case, overrides) => {
      const order = makeTestOrder(overrides);

      await sendOrder(producer, order);
      const processed = await waitForMessage(ORDERS_PROCESSED_TOPIC, byOrderId(order.orderId));

      // toEqual na całym obiekcie: zmienia się tylko status, nic nie znika i nic nie dochodzi.
      expect(processed).toEqual({ ...order, status: "PROCESSED" });
    });

    it("przetwarza wiele wiadomości bez utraty i duplikacji", async () => {
      const orders = Array.from({ length: 10 }, () => makeTestOrder());
      const orderIds = new Set(orders.map((o) => o.orderId));

      await Promise.all(orders.map((o) => sendOrder(producer, o)));
      // Prosimy o jedną wiadomość więcej, niż wysłaliśmy: jeśli pojawi się
      // jedenasta pasująca, to jest duplikat. Test czeka więc pełny timeout.
      const processed = await collectMessages(ORDERS_PROCESSED_TOPIC, byOrderIds(orderIds), orders.length + 1, 10);

      expect(processed).toHaveLength(orders.length);
      expect(new Set(processed.map((p) => p.orderId))).toEqual(orderIds);
      expect(processed.every((p) => p.status === "PROCESSED")).toBe(true);
    });

    it(`przetwarza wiadomość w limicie czasu (SLA ${SLA_MS / 1000}s)`, async () => {
      const order = makeTestOrder();

      const sentAt = Date.now();
      await sendOrder(producer, order);
      const processed = await waitForMessage(ORDERS_PROCESSED_TOPIC, byOrderId(order.orderId), SLA_MS / 1000);
      const elapsedMs = Date.now() - sentAt;

      // Pomiar jest zachowawczy: obejmuje też dołączenie konsumenta testowego do grupy.
      expect(processed).not.toBeNull();
      expect(elapsedMs).toBeLessThanOrEqual(SLA_MS);
    });
  });

  describe("niepoprawne zamówienia trafiają do DLQ", () => {
    it.each(REQUIRED_FIELDS)("wysyła wiadomość bez pola '%s' do DLQ", async (field) => {
      const order = makeTestOrder({ customer: `Klient ${randomUUID()}` });
      const { [field]: _removed, ...invalid } = order;
      // Po czym rozpoznamy wpis na DLQ: po orderId, a gdy to właśnie jego brakuje — po customer.
      const marker = field === "orderId" ? dlqByOriginalField("customer", order.customer) : dlqByOriginalField("orderId", order.orderId);

      await sendRaw(producer, JSON.stringify(invalid), order.orderId);
      const dlq = await waitForMessage(ORDERS_DLQ_TOPIC, marker);

      expect(dlq).not.toBeNull();
      expect(dlq?.error).toContain(`Brakujące pola: [${field}]`);
      expect(dlq?.originalMessage).toEqual(invalid);
      await expectNotProcessed(order.orderId);
    });

    it("wysyła niepoprawny JSON do DLQ", async () => {
      const key = randomUUID();
      const raw = `{"orderId": "${key}", "customer": "Jan`;

      await sendRaw(producer, raw, key);
      const dlq = await waitForMessage(ORDERS_DLQ_TOPIC, dlqByRaw(raw));

      expect(dlq).not.toBeNull();
      expect(dlq?.error).toMatch(/^Niepoprawny JSON/);
      await expectNotProcessed(key);
    });
  });

  describe("walidacja typów i nietypowe dane", () => {
    // Treść tych wiadomości nie zawsze jest unikalna (np. `{}`, `null`), więc
    // „swoją” wiadomość na DLQ rozpoznajemy po unikalnym kluczu — konsument
    // przepisuje klucz z wejścia na wyjście.

    it.each<[string, Record<string, unknown>, string]>([
      ["amount jako string", { amount: "199.99" }, "Pole 'amount' musi być liczbą."],
      ["amount równe null", { amount: null }, "Pole 'amount' musi być liczbą."],
      ["orderId jako liczba", { orderId: 123 }, "Pole 'orderId' musi być stringiem."],
    ])("wysyła do DLQ zamówienie ze złym typem pola (%s)", async (_case, overrides, expectedError) => {
      const key = randomUUID();
      const payload = { ...makeTestOrder({ orderId: key }), ...overrides };

      await sendRaw(producer, JSON.stringify(payload), key);
      const dlq = await waitForMessage(ORDERS_DLQ_TOPIC, withKey(key, isDlqValidationError));

      expect(dlq).not.toBeNull();
      expect(dlq?.error).toBe(expectedError);
      expect(dlq?.originalMessage).toEqual(payload);
      await expectNotProcessed(key);
    });

    it("zgłasza wszystkie błędy walidacji naraz", async () => {
      const key = randomUUID();
      // Brak 'customer' oraz 'amount' złego typu.
      const payload = { orderId: key, amount: "dużo", status: "NEW" };

      await sendRaw(producer, JSON.stringify(payload), key);
      const dlq = await waitForMessage(ORDERS_DLQ_TOPIC, withKey(key, isDlqValidationError));

      expect(dlq).not.toBeNull();
      expect(dlq?.error).toBe("Brakujące pola: [customer]; Pole 'amount' musi być liczbą.");
      await expectNotProcessed(key);
    });

    it("wysyła pusty obiekt JSON do DLQ z listą wszystkich brakujących pól", async () => {
      const key = randomUUID();

      await sendRaw(producer, "{}", key);
      const dlq = await waitForMessage(ORDERS_DLQ_TOPIC, withKey(key, isDlqValidationError));

      expect(dlq).not.toBeNull();
      expect(dlq?.error).toBe("Brakujące pola: [orderId, customer, amount, status]");
      expect(dlq?.originalMessage).toEqual({});
      await expectNotProcessed(key);
    });

    // Poprawny JSON, który nie jest obiektem. Przed poprawką w konsumencie
    // taka wiadomość zatrzymywała przetwarzanie (opis w README).
    it.each(["null", "42", '"tekst"', "true", "[]"])(
      "wysyła do DLQ poprawny JSON, który nie jest obiektem (%s), i przetwarza dalej",
      async (raw) => {
        const key = randomUUID();

        await sendRaw(producer, raw, key);
        const dlq = await waitForMessage(ORDERS_DLQ_TOPIC, withKey(key, isDlqParseError));

        expect(dlq).not.toBeNull();
        expect(dlq?.error).toBe("Wiadomość musi być obiektem JSON.");
        expect(dlq?.raw).toBe(raw);
        // expectNotProcessed czeka na przetworzenie poprawnego zamówienia z tym
        // samym kluczem — przy okazji dowodzi, że konsument nadal działa.
        await expectNotProcessed(key);
      }
    );
  });

  describe("odporność i kontrakt Kafki", () => {
    it("przetwarza poprawne wiadomości przemieszane z błędnymi (konsument nie zatrzymuje się)", async () => {
      const key = randomUUID();
      const [first, second, last] = [makeTestOrder(), makeTestOrder(), makeTestOrder()];
      const brokenJson = `{"orderId": "${key}", broken`;
      const missingAmount = { orderId: randomUUID(), customer: "Bez Kwoty", status: "NEW" };

      // Jedna partycja, ustalona kolejność: dobra, zepsuta, dobra, niepełna, dobra.
      await sendRawBatch(producer, key, [
        JSON.stringify(first),
        brokenJson,
        JSON.stringify(second),
        JSON.stringify(missingAmount),
        JSON.stringify(last),
      ]);

      // Czytamy 'orders-processed' do ostatniego poprawnego zamówienia:
      // wcześniej z tym kluczem mogą być tylko `first` i `second`, w tej kolejności.
      const processed = await collectMessagesUntil(
        ORDERS_PROCESSED_TOPIC,
        withKey(key, isOrder),
        byOrderId(last.orderId)
      );
      expect(processed.stopFound).toBe(true);
      expect(processed.matched).toEqual([
        { ...first, status: "PROCESSED" },
        { ...second, status: "PROCESSED" },
      ]);

      const dlq = await collectMessages(ORDERS_DLQ_TOPIC, withKey(key, anyMessage), 2);
      expect(dlq).toEqual([
        { error: expect.stringMatching(/^Niepoprawny JSON/), raw: brokenJson },
        { error: "Brakujące pola: [amount]", originalMessage: missingAmount },
      ]);
    });

    it("zachowuje kolejność wiadomości o tym samym kluczu", async () => {
      const key = randomUUID();
      const orders = Array.from({ length: 5 }, (_, i) => makeTestOrder({ amount: i + 1 }));

      await sendRawBatch(producer, key, orders.map((o) => JSON.stringify(o)));
      const processed = await collectMessages(ORDERS_PROCESSED_TOPIC, withKey(key, isOrder), orders.length);

      expect(processed.map((p) => p.orderId)).toEqual(orders.map((o) => o.orderId));
    });

    it("zachowuje klucz wiadomości na topicu wyjściowym", async () => {
      const order = makeTestOrder();

      // sendOrder używa orderId jako klucza.
      await sendOrder(producer, order);
      // withKey dopasuje tylko rekord z TYM kluczem — jeśli konsument zgubi albo
      // zmieni klucz, wiadomość z tym orderId nie zostanie znaleziona.
      const processed = await waitForMessage(ORDERS_PROCESSED_TOPIC, withKey(order.orderId, byOrderId(order.orderId)));

      expect(processed).not.toBeNull();
    });

    it("przetwarza dużą wiadomość (~500 KB)", async () => {
      const order = makeTestOrder({ customer: "K".repeat(500_000) });

      await sendOrder(producer, order);
      const processed = await waitForMessage(ORDERS_PROCESSED_TOPIC, byOrderId(order.orderId));

      expect(processed).toEqual({ ...order, status: "PROCESSED" });
    });
  });
});
