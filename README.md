## Wymagania

- Docker + Docker Compose v2 (`docker compose`, nie `docker-compose`).
- Wolny port `9092` lokalnie (Kafka).
- Do pracy lokalnej nad testami (poza Dockerem): Node.js 20+.

## Uruchomienie środowiska

```bash
# Zbuduj obrazy i uruchom Kafkę + inicjalizację topiców + konsumenta
docker compose up --build -d

# Sprawdź logi konsumenta (powinien czekać na wiadomości)
docker compose logs -f consumer
```

## Ręczna demonstracja działania aplikacji (opcjonalnie, nie jest wymagane do zadania)

```bash
docker compose --profile demo run --rm producer
docker compose logs -f consumer
```

## Uruchomienie testów (tu pracujesz)

```bash
docker compose --profile test run --rm tests
```

Na start wszystkie testy w `tests/__tests__/kafkaFlow.test.ts` są oznaczone
`it.skip(...)` — po zaimplementowaniu każdego testu zamień `it.skip` na `it`.

### Uruchomienie testów lokalnie (bez Dockera dla samych testów)

Przydatne przy pisaniu/debugowaniu — szybszy cykl niż przebudowa obrazu Dockera:

```bash
cd tests
npm install
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 npm test
```

Sprawdzenie typów bez uruchamiania testów:

```bash
cd tests
npx tsc --noEmit
```

## Zatrzymanie i sprzątanie środowiska

```bash
docker compose down -v
```

(`-v` czyści też dane Kafki — przydatne, gdy chcesz zacząć testy od zera).


---

## Twoje notatki (uzupełnij przed oddaniem zadania)

### Uwagi do uruchomienia

`docker-compose.yml` jest bez zmian. Testy uruchamiam jedną komendą:

```bash
docker compose up --build -d
docker compose --profile test run --build --rm tests
```

- **`--build` przy `run`**: obraz `tests` kopiuje kod testów w trakcie budowania,
  a samo `run` nie przebudowuje istniejącego obrazu — bez `--build` po zmianie
  testów uruchomiłaby się ich stara wersja.
- **Uruchomienie lokalne (bez Dockera dla testów)**: broker ogłasza adres
  `kafka:9092` (`KAFKA_ADVERTISED_LISTENERS`), więc z hosta połączenie przez
  `localhost:9092` kończy się błędem `getaddrinfo ENOTFOUND kafka`. Obejście bez
  zmiany konfiguracji: dodać `127.0.0.1 kafka` do `/etc/hosts`.
- Pojedynczy test: `docker compose --profile test run --build --rm tests npx jest -t "bez pola 'amount'"`.
- Losowa kolejność (sprawdzenie izolacji): `docker compose --profile test run --build --rm tests npx jest --runInBand --randomize`.

#### Zmiany w `tests/src/kafkaHelpers.ts`

Publiczne funkcje mają te same nazwy i parametry; zmiany:

- **Typowanie zamiast `any`**: `MatchFn<T>` jest type guardem, więc
  `waitForMessage` / `collectMessages` zwracają wiadomość konkretnego typu
  (`Order`, `DlqValidationError`, `DlqParseError`). Doszły gotowe matchery
  (`byOrderId`, `byOrderIds`, `dlqByOrderId`, `dlqByOriginalField`, `dlqByRaw`,
  `isDlqValidationError`, `isDlqParseError`).
- **Dopasowanie po kluczu rekordu (`withKey`)**: matcher dostaje też klucz wiadomości.
  Konsument przepisuje klucz z wejścia na wyjście, więc wysyłając wiadomość z unikalnym
  kluczem, test znajduje „swój” wpis na DLQ nawet wtedy, gdy treść nie jest unikalna
  (`{}`, `null`, `[]`). Bez tego test mógłby przejść na wpisie z wcześniejszego uruchomienia.
- **`sendRawBatch`** — kilka wiadomości z jednym kluczem w jednym `send`, czyli na jedną
  partycję w ustalonej kolejności (testy kolejności i przemieszanych dobrych/złych wiadomości);
  **`isOrder`** — type guard sprawdzający kształt zamówienia.
- **Jawny partitioner** (`Partitioners.DefaultPartitioner`, ten sam co w aplikacji) —
  ten sam klucz zawsze trafia na tę samą partycję; usuwa też ostrzeżenie kafkajs.
- **Jeden wspólny mechanizm czytania** (`consumeUntil`) zamiast dwóch skopiowanych
  implementacji. Konsument jest rozłączany zawsze — także przy błędzie (wcześniej
  przy `reject` zostawał podłączony) — i poza callbackiem `eachMessage`.
- **`collectMessagesUntil`** — czyta topic do momentu pojawienia się wiadomości-znacznika
  (sentinel). Pozwala sprawdzić, że wiadomość *nie* trafiła na `orders-processed`,
  bez czekania pełnego timeoutu (opis w komentarzu funkcji).
- **Domyślny timeout odczytu 10 s zamiast 20 s** — przetwarzanie trwa milisekundy, a dwa odczyty
  w jednym teście (wynik + sentinel) mieszczą się w limicie Jesta (30 s). Gdy coś nie działa,
  test kończy się czytelną asercją, a nie ogólnym „Exceeded timeout”.
- **Uczciwy typ matcherów po `orderId`** (`WithOrderId`): sprawdzają tylko `orderId`, więc nie
  deklarują pełnego typu `Order`. To celowe — wiadomość z błędnymi danymi ma zostać znaleziona,
  żeby `toEqual` pokazało różnicę. Pełny kształt sprawdza `isOrder`.
- **`logLevel: WARN`** — domyślne logi INFO kafkajs zasłaniały wynik testów.

#### Poprawka w konsumencie: JSON, który nie jest obiektem

**Problem.** Wiadomość będąca poprawnym JSON-em, ale nie obiektem (np. `null`, `42`,
`"abc"`), przechodziła przez `JSON.parse` bez błędu i trafiała do `validate()`.
Tam operator `in` (`"orderId" in order`) rzucał `TypeError`, którego nikt nie łapał.
kafkajs ponawiał tę samą wiadomość, po czym zatrzymywał konsumenta — proces Node
działał dalej (Docker go nie restartował), ale **żadne kolejne zamówienie nie było
już przetwarzane**.

**Jak odtworzyć (przed poprawką):**

```bash
echo 'null' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-processing-service
# -> brak CONSUMER-ID na partycjach, LAG rośnie, nowe zamówienia nie są przetwarzane
```

**Poprawka** (`consumer/src/consumer.ts`). Przed walidacją sprawdzam, czy sparsowana
wartość jest obiektem (nie `null` i nie tablicą). Jeśli nie, wiadomość trafia na
`orders-dlq` jako `{ "error": "Wiadomość musi być obiektem JSON.", "raw": "<oryginał>" }`
— w tym samym formacie co zepsuty JSON. Pozostałe zachowanie konsumenta jest bez zmian.

**Dlaczego poprawiam, a nie tylko opisuję:** bez poprawki test tego przypadku
zatrzymałby konsumenta i zepsuł wszystkie kolejne testy (brak izolacji).

**Weryfikacja:** testy „wysyła do DLQ poprawny JSON, który nie jest obiektem” uruchomione na
konsumencie sprzed poprawki kończą się porażką (wszystkie 5: `Received: null` — wpis na DLQ
się nie pojawia), a na poprawionym przechodzą.

Po zmianie konsumenta trzeba przebudować jego obraz: `docker compose up --build -d consumer`.

### Podejście do testowania

Testy są czarnoskrzynkowe (E2E): wysyłają wiadomości na `orders` i sprawdzają wynik na
`orders-processed` / `orders-dlq`, tak jak widzi go każdy inny odbiorca tych topiców.
Izolację zapewniają unikalne identyfikatory (UUID jako `orderId` i jako klucz wiadomości) —
każdy test szuka tylko swoich wiadomości, więc testy można uruchamiać pojedynczo, w losowej
kolejności i wielokrotnie na tym samym środowisku. Synchronizacja opiera się wyłącznie na
pollingu z timeoutem, a to, że wiadomość *nie* trafiła na topic, sprawdzam wzorcem „sentinel”
(poprawna wiadomość z tym samym kluczem wysłana po sprawdzanej — dzięki kolejności w partycji
nie trzeba czekać pełnego timeoutu). Przypadki brzegowe są sparametryzowane (`it.each`),
a asercje porównują całe obiekty (`toEqual`), żeby wychwycić także zgubione lub dodane pola.

**Pokrycie (27 testów):** happy path i zachowanie danych (polskie znaki, `amount` 0 /
ułamki / duże kwoty), 10 wiadomości bez utraty i duplikacji, SLA 10 s, brak każdego
z wymaganych pól, zepsuty JSON, złe typy pól, kilka błędów naraz, `{}`, JSON niebędący
obiektem (`null`, `42`, `"tekst"`, `true`, `[]`), przemieszane dobre i złe wiadomości,
kolejność w obrębie klucza, zachowanie klucza, wiadomość ~500 KB.

### Znane ograniczenia / co zrobiłabym inaczej mając więcej czasu

**Zachowanie aplikacji do wyjaśnienia z autorem wymagań** (z analizy kodu konsumenta;
nie zamieniałam tego w testy, bo specyfikacja nie rozstrzyga oczekiwanego wyniku):

- Walidacja nie sprawdza typu `customer` i `status` ani wartości pól — np. `customer: 123`,
  `customer: null`, `amount: -5` czy `status: "CANCELLED"` trafią na `orders-processed`.
- Dodatkowe pola wiadomości są gubione (konsument buduje nowy obiekt z 4 pól), choć opis
  zadania mówi o „tej samej wiadomości” ze zmienionym statusem.
- Wiadomość z pustą wartością (`value = null`, tzw. tombstone) jest pomijana bez śladu —
  nie trafia ani na wyjście, ani na DLQ.
- Poprawka w konsumencie dotyczy tylko JSON-a niebędącego obiektem. Inny nieobsłużony wyjątek
  w `eachMessage` (np. błąd wysyłki na DLQ) nadal może zatrzymać przetwarzanie — warto dodać
  ogólną obsługę błędów i kończenie procesu, gdy klient kafkajs przestanie działać.

**Testy i infrastruktura:**

- Test „bez duplikacji” zawsze czeka pełne 10 s (czeka na ewentualną 11. wiadomość)
  i wykrywa duplikaty tylko w tym oknie. Nie testuję duplikatów przy restarcie konsumenta
  (semantyka at-least-once) — to byłby kolejny scenariusz, np. `docker restart consumer` w trakcie testu.
- Każdy odczyt czyta topic od początku, więc czas testów rośnie z ilością danych na topicach.
  Lepiej byłoby zapamiętać offsety przed wysłaniem (`admin.fetchTopicOffsets`) i czytać od nich.
- Grupy konsumentów `test-*` nie są usuwane po testach (`admin.deleteGroups`).
- Pomiar SLA jest zachowawczy — obejmuje też dołączenie konsumenta testowego do grupy.
  Dokładniej byłoby porównać znaczniki czasu rekordów wejściowego i wyjściowego.
- Testy zakładają działające środowisko (`docker compose up`). Z większą ilością czasu:
  Testcontainers albo `globalSetup` sprawdzający gotowość, oraz uruchamianie w CI.
- Uruchomienie lokalne wymaga wpisu w `/etc/hosts` — docelowo drugi listener
  (`EXTERNAL://localhost:9094`) w `docker-compose.yml`, świadomie niezmienianym w tym zadaniu.
- Brak testów kontraktu (JSON Schema dla wiadomości) i testów obciążeniowych.
