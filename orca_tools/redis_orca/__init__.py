import redis


def get_connection() -> redis.Redis:
    return redis.Redis(
        host="redis",
        port=6379,
        db=0,
        decode_responses=True,
    )




def main() -> None:
    conn = get_connection()
    conn.set("key", "value")
    print(conn.get("key"))
    conn.delete("key")
    print(conn.get("key"))

if __name__ == "__main__":
    main()
