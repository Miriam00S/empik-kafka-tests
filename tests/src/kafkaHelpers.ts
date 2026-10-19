/**
 * Helpery do testów Kafki.
 *
 * Do Twojej dyspozycji:
 *   - getProducer() / disconnectProducer() — połączony klient producenta.
 *   - makeTestOrder(overrides) — buduje przykładowe, poprawne zamówienie.
 *   - sendOrder(producer, order, topic?) — wysyła zamówienie (obiekt) na topic.
 *   - sendRaw(producer, rawValue, key?, topic?) — wysyła surowe dane
 *     (przydatne np. do testowania niepoprawnego JSON-a).
 *   - sendRawBatch(producer, key, rawValues, topic?) — wysyła kilka wiadomości
 *     z jednym kluczem, w zadanej kolejności.
 *   - waitForMessage(topic, matchFn, timeoutSeconds?) — czeka aż na danym
 *     topicu pojawi się wiadomość spełniająca matchFn(value).
 *     Zwraca sparsowany obiekt albo null, jeśli nie znaleziono w limicie czasu.
 *   - collectMessages(topic, matchFn, expectedCount, timeoutSeconds?)
 *     — zbiera do expectedCount wiadomości spełniających matchFn.
 *   - collectMessagesUntil(topic, matchFn, stopFn, timeoutSeconds?)
 *     — zbiera wiadomości spełniające matchFn, dopóki nie pojawi się wiadomość
 *     spełniająca stopFn (tzw. sentinel). Służy do sprawdzania, że czegoś
 *     NIE ma na topicu, bez czekania na pełny timeout.
 *   - byOrderId / byOrderIds / dlqByOriginalField / dlqByOrderId / dlqByRaw /
 *     isOrder / isDlqValidationError / isDlqParseError / anyMessage
 *     — gotowe, typowane funkcje dopasowujące (type guardy).
 *   - withKey(key, matchFn) — zawęża dopasowanie do rekordów o danym kluczu.
 *
 * Każde wywołanie waitForMessage / collectMessages* tworzy konsumenta
 * z UNIKALNYM groupId i czyta topic od początku (fromBeginning: true)
 * — dzięki temu testy są od siebie izolowane i nie interferują ze sobą,
 * niezależnie od kolejności uruchomienia.
 */
import { Kafka, Producer, Consumer, EachMessagePayload, Partitioners, logLevel } from "kafkajs";
import { randomUUID } from "crypto";

export const BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092";
export const ORDERS_TOPIC = "orders";
export const ORDERS_PROCESSED_TOPIC = "orders-processed";
export const ORDERS_DLQ_TOPIC = "orders-dlq";

const DEFAULT_POLL_TIMEOUT_SECONDS = 20;

const kafka = new Kafka({
  clientId: "kafka-qa-tests",
  // Domyślny poziom INFO zalewa wynik Jesta logami o dołączaniu do grup.
  logLevel: logLevel.WARN,
  brokers: [BOOTSTRAP_SERVERS],
  retry: {
    initialRetryTime: 2000,
    retries: 30,
  },
});

export interface Order {
  orderId: string;
  customer: string;
  amount: number;
  status: string;
}

/** Wiadomość na DLQ dla poprawnego JSON-a, który nie przeszedł walidacji pól. */
export interface DlqValidationError {
  error: string;
  originalMessage: Record<string, unknown>;
}

/** Wiadomość na DLQ dla danych, które nie są obiektem JSON (zepsuty JSON, `null`, liczba...). */
export interface DlqParseError {
  error: string;
  raw: string;
}

/** Metadane rekordu Kafki przekazywane do funkcji dopasowującej. */
export interface RecordMeta {
  key: string | null;
}

/**
 * Funkcja dopasowująca w postaci type guarda: jeśli zwróci true, helper
 * wie, jakiego typu jest wiadomość, i zwraca ją z tym typem (bez `any`).
 * Drugi argument (klucz rekordu) jest opcjonalny w użyciu — patrz withKey().
 */
export type MatchFn<T> = (value: unknown, meta: RecordMeta) => value is T;

export function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Akceptuje dowolną wiadomość — do użycia z withKey(), gdy liczy się tylko klucz. */
export function anyMessage(_value: unknown): _value is unknown {
  return true;
}

/**
 * Zawęża dopasowanie do rekordów o danym kluczu. Konsument przepisuje klucz
 * z wejścia na wyjście, więc unikalny klucz pozwala znaleźć „swoją” wiadomość
 * nawet wtedy, gdy jej treść nie jest unikalna (np. `{}`, `null`, `[]`).
 */
export function withKey<T>(key: string, matchFn: MatchFn<T>): MatchFn<T> {
  return (value, meta): value is T => meta.key === key && matchFn(value, meta);
}

/** Sprawdza, czy wartość ma kształt zamówienia (wszystkie pola z poprawnymi typami). */
export function isOrder(value: unknown): value is Order {
  return (
    isObject(value) &&
    typeof value.orderId === "string" &&
    typeof value.customer === "string" &&
    typeof value.amount === "number" &&
    typeof value.status === "string"
  );
}

export function isDlqValidationError(value: unknown): value is DlqValidationError {
  return isObject(value) && typeof value.error === "string" && isObject(value.originalMessage);
}

export function isDlqParseError(value: unknown): value is DlqParseError {
  return isObject(value) && typeof value.error === "string" && typeof value.raw === "string";
}

/** Dopasowuje zamówienie o danym orderId (np. na 'orders-processed'). */
export function byOrderId(orderId: string): MatchFn<Order> {
  return (value): value is Order => isObject(value) && value.orderId === orderId;
}

/** Dopasowuje zamówienia, których orderId należy do podanego zbioru. */
export function byOrderIds(orderIds: ReadonlySet<string>): MatchFn<Order> {
  return (value): value is Order =>
    isObject(value) && typeof value.orderId === "string" && orderIds.has(value.orderId);
}

/** Dopasowuje wpis na DLQ, którego originalMessage[field] === expected. */
export function dlqByOriginalField(field: string, expected: unknown): MatchFn<DlqValidationError> {
  return (value): value is DlqValidationError =>
    isDlqValidationError(value) && value.originalMessage[field] === expected;
}

/** Dopasowuje wpis na DLQ dla zamówienia o danym orderId. */
export function dlqByOrderId(orderId: string): MatchFn<DlqValidationError> {
  return dlqByOriginalField("orderId", orderId);
}

/** Dopasowuje wpis na DLQ dla nieparsowalnej wiadomości o dokładnie takiej treści. */
export function dlqByRaw(raw: string): MatchFn<DlqParseError> {
  return (value): value is DlqParseError => isDlqParseError(value) && value.raw === raw;
}

let sharedProducer: Producer | null = null;

/** Zwraca połączonego, współdzielonego producenta (tworzy go przy pierwszym wywołaniu). */
export async function getProducer(): Promise<Producer> {
  if (!sharedProducer) {
    // Jawnie domyślny partitioner kafkajs v2 (ten sam co w aplikacji) — ten sam
    // klucz zawsze trafia na tę samą partycję; wycisza też ostrzeżenie o zmianie domyślnego.
    sharedProducer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
    await sharedProducer.connect();
  }
  return sharedProducer;
}

/** Rozłącza współdzielonego producenta — wywołaj w afterAll(). */
export async function disconnectProducer(): Promise<void> {
  if (sharedProducer) {
    await sharedProducer.disconnect();
    sharedProducer = null;
  }
}

/** Buduje przykładowe, poprawne zamówienie testowe z możliwością nadpisania pól. */
export function makeTestOrder(overrides: Partial<Order> = {}): Order {
  return {
    orderId: randomUUID(),
    customer: "Test Testowski",
    amount: 123.45,
    status: "NEW",
    ...overrides,
  };
}

/** Wysyła zamówienie (obiekt) jako JSON na wskazany topic (domyślnie 'orders'). */
export async function sendOrder(
  producer: Producer,
  order: Partial<Order>,
  topic: string = ORDERS_TOPIC
): Promise<void> {
  await producer.send({
    topic,
    messages: [{ key: order.orderId, value: JSON.stringify(order) }],
  });
}

/**
 * Wysyła surowe (potencjalnie niepoprawne) dane, np. zepsuty JSON.
 * Przydatne do testowania obsługi błędnych wiadomości.
 */
export async function sendRaw(
  producer: Producer,
  rawValue: string | Buffer,
  key?: string,
  topic: string = ORDERS_TOPIC
): Promise<void> {
  await producer.send({
    topic,
    messages: [{ key, value: rawValue }],
  });
}

/**
 * Wysyła kilka surowych wiadomości z tym samym kluczem w JEDNYM wywołaniu send.
 * Ten sam klucz = ta sama partycja, a jedno wywołanie gwarantuje, że trafią
 * do niej dokładnie w podanej kolejności (przydatne w testach kolejności).
 */
export async function sendRawBatch(
  producer: Producer,
  key: string,
  rawValues: string[],
  topic: string = ORDERS_TOPIC
): Promise<void> {
  await producer.send({
    topic,
    messages: rawValues.map((value) => ({ key, value })),
  });
}

function newConsumer(): Consumer {
  return kafka.consumer({ groupId: `test-${randomUUID()}` });
}

/** Zwraca sparsowany JSON albo undefined, gdy wartość nie jest poprawnym JSON-em. */
function tryParse(value: Buffer | null): unknown {
  if (!value) return undefined;
  try {
    return JSON.parse(value.toString("utf-8"));
  } catch {
    return undefined;
  }
}

/**
 * Wspólny silnik dla wszystkich funkcji czytających: czyta topic od początku
 * nowym konsumentem i przekazuje każdą sparsowaną wiadomość (wraz z kluczem) do onMessage.
 * Kończy, gdy onMessage zwróci true albo minie timeout.
 *
 * Konsument jest rozłączany zawsze (także przy błędzie) i POZA eachMessage —
 * rozłączanie z wnętrza eachMessage czeka na zakończenie tego samego callbacku.
 */
async function consumeUntil(
  topic: string,
  onMessage: (value: unknown, meta: RecordMeta) => boolean,
  timeoutSeconds: number
): Promise<void> {
  const consumer = newConsumer();
  let timer: NodeJS.Timeout | undefined;

  try {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    await new Promise<void>((resolve, reject) => {
      let done = false;
      const stop = () => {
        done = true;
        resolve();
      };
      timer = setTimeout(stop, timeoutSeconds * 1000);

      consumer
        .run({
          eachMessage: async ({ message }: EachMessagePayload) => {
            if (done) return;
            const parsed = tryParse(message.value);
            const meta: RecordMeta = { key: message.key ? message.key.toString("utf-8") : null };
            if (parsed !== undefined && onMessage(parsed, meta)) {
              stop();
            }
          },
        })
        .catch(reject);
    });
  } finally {
    clearTimeout(timer);
    await consumer.disconnect();
  }
}

/**
 * Czeka aż w danym topicu pojawi się wiadomość spełniająca matchFn.
 * Zwraca sparsowaną wiadomość albo null, jeśli nie znaleziono w limicie czasu.
 */
export async function waitForMessage<T>(
  topic: string,
  matchFn: MatchFn<T>,
  timeoutSeconds: number = DEFAULT_POLL_TIMEOUT_SECONDS
): Promise<T | null> {
  let found: T | null = null;
  await consumeUntil(
    topic,
    (value, meta) => {
      if (!matchFn(value, meta)) return false;
      found = value;
      return true;
    },
    timeoutSeconds
  );
  return found;
}

/**
 * Zbiera do expectedCount wiadomości spełniających matchFn w limicie czasu.
 *
 * Uwaga: funkcja kończy się po znalezieniu expectedCount wiadomości, więc sama
 * nie wykryje duplikatów. Aby je wykryć, poproś o (oczekiwana liczba + 1)
 * i sprawdź, że wróciło dokładnie tyle, ile oczekujesz.
 */
export async function collectMessages<T>(
  topic: string,
  matchFn: MatchFn<T>,
  expectedCount: number,
  timeoutSeconds: number = DEFAULT_POLL_TIMEOUT_SECONDS
): Promise<T[]> {
  const found: T[] = [];
  await consumeUntil(
    topic,
    (value, meta) => {
      if (!matchFn(value, meta)) return false;
      found.push(value);
      return found.length >= expectedCount;
    },
    timeoutSeconds
  );
  return found;
}

export interface CollectUntilResult<T> {
  /** Wiadomości spełniające matchFn, które pojawiły się PRZED sentinelem. */
  matched: T[];
  /** Czy sentinel został znaleziony (false = timeout, wynik niemiarodajny). */
  stopFound: boolean;
}

/**
 * Zbiera wiadomości spełniające matchFn, dopóki nie pojawi się wiadomość
 * spełniająca stopFn (sentinel).
 *
 * Kafka gwarantuje kolejność tylko w obrębie partycji, więc sentinel musi być
 * wysłany z TYM SAMYM kluczem co sprawdzana wiadomość i PO niej. Wtedy, gdy
 * sentinel pojawi się na wyjściu, konsument na pewno obsłużył już wcześniejszą
 * wiadomość — i jeśli nie ma jej w `matched`, to znaczy, że tu nie trafiła.
 */
export async function collectMessagesUntil<T>(
  topic: string,
  matchFn: MatchFn<T>,
  stopFn: MatchFn<unknown>,
  timeoutSeconds: number = DEFAULT_POLL_TIMEOUT_SECONDS
): Promise<CollectUntilResult<T>> {
  const matched: T[] = [];
  let stopFound = false;
  await consumeUntil(
    topic,
    (value, meta) => {
      if (stopFn(value, meta)) {
        stopFound = true;
        return true;
      }
      if (matchFn(value, meta)) matched.push(value);
      return false;
    },
    timeoutSeconds
  );
  return { matched, stopFound };
}
