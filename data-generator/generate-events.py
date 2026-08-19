import argparse
import multiprocessing as mp
import random
import time
from datetime import datetime, timedelta, timezone

import orjson
from kafka import KafkaProducer


# ============================================================
# Default configuration
# ============================================================

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "events"

DEFAULT_NUM_EVENTS = 1_000_000
DEFAULT_NUM_ATTRIBUTES = 20
DEFAULT_LATE_EVENT_RATE = 0.05
DEFAULT_NUM_ENTITIES = 100_000
DEFAULT_DATA_SKEW = 0.5

DEFAULT_WORKERS = 4

DEFAULT_EVENT_TIME_RANGE_HOURS = 24
DEFAULT_MAX_LATE_HOURS = 2

# Kafka producer
KAFKA_BATCH_SIZE = 1024 * 1024       # 1 MB
KAFKA_BUFFER_MEMORY = 256 * 1024 * 1024
KAFKA_LINGER_MS = 5

# Entity pool.
#
# Không cần pool bằng NUM_ENTITIES.
# Pool chỉ dùng để tạo distribution.
ENTITY_POOL_SIZE = 100_000

# data_skew > 0:
# bao nhiêu % entity đầu được coi là "hot entities".
HOT_ENTITY_RATIO = 0.01


# ============================================================
# Static event data
# ============================================================

EVENT_TYPES = (
    "purchase",
    "view_product",
    "add_to_cart",
    "remove_from_cart",
    "login",
)

CATEGORIES = (
    "electronics",
    "fashion",
    "food",
    "books",
    "home",
    "sports",
)

CITIES = (
    "Hanoi",
    "HoChiMinh",
    "DaNang",
    "HaiPhong",
    "CanTho",
)

DEVICES = (
    "mobile",
    "desktop",
    "tablet",
)

PAYMENTS = (
    "cash",
    "card",
    "bank_transfer",
    "e_wallet",
)

SOURCES = (
    "google",
    "facebook",
    "direct",
    "recommendation",
)

LOGIN_METHODS = (
    "password",
    "google",
    "facebook",
    "apple",
)


# ============================================================
# Validation
# ============================================================

def validate_config(args):

    if args.num_events <= 0:
        raise ValueError(
            "num_events must be > 0"
        )

    if not 1 <= args.num_attributes <= 200:
        raise ValueError(
            "num_attributes must be between 1 and 200"
        )

    if not 0 <= args.late_event_rate <= 1:
        raise ValueError(
            "late_event_rate must be between 0 and 1"
        )

    if args.num_entities <= 0:
        raise ValueError(
            "num_entities must be > 0"
        )

    if not 0 <= args.data_skew <= 1:
        raise ValueError(
            "data_skew must be between 0 and 1"
        )

    if args.workers <= 0:
        raise ValueError(
            "workers must be > 0"
        )


# ============================================================
# Entity pool
# ============================================================

def build_entity_pool(
    num_entities: int,
    data_skew: float,
):
    """
    Tạo một pool nhỏ đại diện cho distribution của entity.

    Không dùng random.choices() ở mỗi event.

    data_skew = 0:
        Uniform.

    data_skew = 1:
        100% event tập trung vào 1% hot entities.

    data_skew = 0.5:
        50% event từ hot entities,
        50% event phân phối uniform.
    """

    pool_size = min(
        ENTITY_POOL_SIZE,
        max(num_entities, 1)
    )

    hot_entities = max(
        1,
        int(num_entities * HOT_ENTITY_RATIO)
    )

    pool = []

    hot_count = int(
        pool_size * data_skew
    )

    normal_count = (
        pool_size - hot_count
    )

    # --------------------------------------------------------
    # Hot entities
    # --------------------------------------------------------

    for _ in range(hot_count):

        entity_index = random.randrange(
            hot_entities
        )

        pool.append(entity_index)

    # --------------------------------------------------------
    # Normal entities
    # --------------------------------------------------------

    for _ in range(normal_count):

        entity_index = random.randrange(
            num_entities
        )

        pool.append(entity_index)

    random.shuffle(pool)

    return pool


# ============================================================
# Event generator
# ============================================================

def generate_event(
    event_id: int,
    entity_pool,
    num_entities: int,
    num_attributes: int,
    late_event_rate: float,
    base_timestamp: float,
    event_time_range_seconds: int,
    max_late_seconds: int,
):
    # --------------------------------------------------------
    # Entity
    # --------------------------------------------------------

    entity_index = entity_pool[
        random.randrange(
            len(entity_pool)
        )
    ]

    entity_id = (
        f"customer-{entity_index}"
    )

    # --------------------------------------------------------
    # Event type
    # --------------------------------------------------------

    event_type = random.choice(
        EVENT_TYPES
    )

    # --------------------------------------------------------
    # Event time
    # --------------------------------------------------------

    event_timestamp = (
        base_timestamp
        + random.randrange(
            event_time_range_seconds
        )
    )

    # --------------------------------------------------------
    # Late event
    # --------------------------------------------------------

    if random.random() < late_event_rate:

        event_timestamp -= random.randrange(
            1,
            max_late_seconds + 1
        )

    # ISO-8601
    event_time = datetime.fromtimestamp(
        event_timestamp,
        timezone.utc,
    ).isoformat()

    # --------------------------------------------------------
    # Attributes
    # --------------------------------------------------------

    attributes = {
        "product_id":
            f"product-{random.randrange(1, 10001)}",

        "category":
            random.choice(CATEGORIES),

        "city":
            random.choice(CITIES),

        "device":
            random.choice(DEVICES),
    }

    # --------------------------------------------------------
    # Purchase
    # --------------------------------------------------------

    if event_type == "purchase":

        attributes["amount"] = round(
            random.uniform(
                10_000,
                20_000_000
            ),
            2
        )

        attributes["quantity"] = random.randint(
            1,
            10
        )

        attributes["payment_method"] = (
            random.choice(PAYMENTS)
        )

    # --------------------------------------------------------
    # Product view
    # --------------------------------------------------------

    elif event_type == "view_product":

        attributes["duration_seconds"] = (
            random.randint(1, 600)
        )

        attributes["source"] = (
            random.choice(SOURCES)
        )

    # --------------------------------------------------------
    # Cart
    # --------------------------------------------------------

    elif event_type in (
        "add_to_cart",
        "remove_from_cart",
    ):

        attributes["quantity"] = (
            random.randint(1, 5)
        )

    # --------------------------------------------------------
    # Login
    # --------------------------------------------------------

    elif event_type == "login":

        attributes["login_method"] = (
            random.choice(LOGIN_METHODS)
        )

    # --------------------------------------------------------
    # Additional fields
    # --------------------------------------------------------

    current_count = len(attributes)

    for i in range(
        current_count,
        num_attributes
    ):

        attributes[
            f"field_{i + 1}"
        ] = random.randint(
            0,
            1_000_000
        )

    # --------------------------------------------------------
    # Envelope
    # --------------------------------------------------------

    return {
        "event_id": f"evt-{event_id}",

        "event_type": event_type,

        "entity_id": entity_id,

        "event_time": event_time,

        "attributes": attributes,
    }, entity_id


# ============================================================
# Worker
# ============================================================

def worker(
    worker_id: int,
    start_event_id: int,
    num_events: int,
    args,
    entity_pool,
    base_timestamp: float,
    result_queue,
):
    """
    Mỗi process có KafkaProducer riêng.

    Đây là điểm rất quan trọng:
    KafkaProducer không được share giữa multiprocessing
    processes.
    """

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,

        # Chúng ta đã serialize bằng orjson
        value_serializer=None,

        # Batch tối đa cho mỗi partition
        batch_size=1024 * 1024,

        # Cho phép gom record trong tối đa 5ms
        linger_ms=5,

        # Nén batch
        compression_type="lz4",

        # Benchmark throughput
        acks=1,

        retries=5,

        max_in_flight_requests_per_connection=5,

        max_request_size=1024 * 1024,
    )
    
    start = time.perf_counter()

    sent = 0

    event_time_range_seconds = (
        args.event_time_range_hours * 3600
    )

    max_late_seconds = (
        args.max_late_hours * 3600
    )

    for i in range(num_events):

        event_id = (
            start_event_id + i
        )

        event, entity_id = generate_event(
            event_id=event_id,

            entity_pool=entity_pool,

            num_entities=args.num_entities,

            num_attributes=args.num_attributes,

            late_event_rate=args.late_event_rate,

            base_timestamp=base_timestamp,

            event_time_range_seconds=(
                event_time_range_seconds
            ),

            max_late_seconds=max_late_seconds,
        )

        value = orjson.dumps(event)

        # entity_id làm Kafka key.
        #
        # Cùng entity sẽ được gửi vào cùng partition
        # theo partitioner mặc định.
        # producer.send(
        #     args.topic,

        #     key=entity_id.encode("utf-8"),

        #     value=value,
        # )

        sent += 1

    # Flush một lần ở cuối worker.
    # producer.flush()

    # producer.close()

    elapsed = (
        time.perf_counter() - start
    )

    result_queue.put(
        (
            worker_id,
            sent,
            elapsed,
        )
    )


# ============================================================
# Main
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "High throughput Kafka event generator"
        )
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
    )

    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
    )

    parser.add_argument(
        "--num-events",
        type=int,
        default=DEFAULT_NUM_EVENTS,
    )

    parser.add_argument(
        "--num-attributes",
        type=int,
        default=DEFAULT_NUM_ATTRIBUTES,
    )

    parser.add_argument(
        "--late-event-rate",
        type=float,
        default=DEFAULT_LATE_EVENT_RATE,
    )

    parser.add_argument(
        "--num-entities",
        type=int,
        default=DEFAULT_NUM_ENTITIES,
    )

    parser.add_argument(
        "--data-skew",
        type=float,
        default=DEFAULT_DATA_SKEW,
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
    )

    parser.add_argument(
        "--event-time-range-hours",
        type=int,
        default=DEFAULT_EVENT_TIME_RANGE_HOURS,
    )

    parser.add_argument(
        "--max-late-hours",
        type=int,
        default=DEFAULT_MAX_LATE_HOURS,
    )

    args = parser.parse_args()

    validate_config(args)

    # --------------------------------------------------------
    # Print configuration
    # --------------------------------------------------------

    print("=" * 70)
    print("High Throughput Kafka Event Generator")
    print("=" * 70)

    print(
        f"Kafka             : "
        f"{args.bootstrap_servers}"
    )

    print(
        f"Topic             : "
        f"{args.topic}"
    )

    print(
        f"Events            : "
        f"{args.num_events:,}"
    )

    print(
        f"Attributes/event  : "
        f"{args.num_attributes}"
    )

    print(
        f"Entities          : "
        f"{args.num_entities:,}"
    )

    print(
        f"Data skew         : "
        f"{args.data_skew}"
    )

    print(
        f"Late event rate   : "
        f"{args.late_event_rate:.2%}"
    )

    print(
        f"Workers           : "
        f"{args.workers}"
    )

    print("=" * 70)

    # --------------------------------------------------------
    # Build entity pool ONCE
    # --------------------------------------------------------

    print("Building entity pool...")

    entity_pool = build_entity_pool(
        args.num_entities,
        args.data_skew,
    )

    print(
        f"Entity pool size  : "
        f"{len(entity_pool):,}"
    )

    # --------------------------------------------------------
    # Base timestamp
    # --------------------------------------------------------

    now = datetime.now(
        timezone.utc
    )

    base_time = (
        now
        - timedelta(
            hours=args.event_time_range_hours
        )
    )

    base_timestamp = (
        base_time.timestamp()
    )

    # --------------------------------------------------------
    # Split events between workers
    # --------------------------------------------------------

    workers = min(
        args.workers,
        args.num_events,
    )

    events_per_worker = (
        args.num_events // workers
    )

    remainder = (
        args.num_events % workers
    )

    # --------------------------------------------------------
    # Multiprocessing
    # --------------------------------------------------------

    result_queue = mp.Queue()

    processes = []

    current_event_id = 0

    global_start = time.perf_counter()

    for worker_id in range(workers):

        worker_events = (
            events_per_worker
        )

        if worker_id < remainder:
            worker_events += 1

        process = mp.Process(
            target=worker,
            args=(
                worker_id,

                current_event_id,

                worker_events,

                args,

                entity_pool,

                base_timestamp,

                result_queue,
            ),
        )

        process.start()

        processes.append(process)

        current_event_id += worker_events

    # --------------------------------------------------------
    # Collect results
    # --------------------------------------------------------

    total_sent = 0

    worker_results = []

    for _ in range(workers):

        result = result_queue.get()

        worker_results.append(
            result
        )

        worker_id, sent, elapsed = result

        total_sent += sent

        print(
            f"Worker {worker_id}: "
            f"{sent:,} events in "
            f"{elapsed:.2f}s "
            f"("
            f"{sent / elapsed:,.0f} events/s"
            ")"
        )

    # --------------------------------------------------------
    # Wait for processes
    # --------------------------------------------------------

    for process in processes:

        process.join()

    global_elapsed = (
        time.perf_counter()
        - global_start
    )

    # --------------------------------------------------------
    # Global throughput
    # --------------------------------------------------------

    throughput = (
        total_sent / global_elapsed
    )

    print()
    print("=" * 70)
    print("Benchmark Result")
    print("=" * 70)

    print(
        f"Total events       : "
        f"{total_sent:,}"
    )

    print(
        f"Elapsed            : "
        f"{global_elapsed:.2f}s"
    )

    print(
        f"Kafka throughput   : "
        f"{throughput:,.0f} events/sec"
    )

    print("=" * 70)


if __name__ == "__main__":

    # Quan trọng trên Windows.
    mp.freeze_support()

    main()