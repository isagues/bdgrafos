CREATE TABLE webgraph1 (src INTEGER, dst INTEGER);
CREATE TABLE webgraph2 (src INTEGER, dst INTEGER);
CREATE TABLE webgraph3 (src INTEGER, dst INTEGER);
select count(*)
from webgraph3;
-- Use Case A
explain ANALYSE select w.src,
    w.dst,
    1,
    concat(w.src::text, '-', w.dst::text)
from webgraph3 w
where w.src != w.dst;
-- Use Case B
explain ANALYSE select w1.src,
    w2.dst,
    2,
    concat(
        w1.src::text,
        '-',
        w1.dst::text,
        '-',
        w2.dst::text
    )
from webgraph1 w1
    inner JOIN webgraph1 w2 on w1.dst = w2.src
where w1.src != w2.dst;
-- Use Case C
explain ANALYSE select w1.src,
    w3.dst,
    3,
    concat(
        w1.src::text,
        '-',
        w1.dst::text,
        '-',
        w2.dst::text,
        '-',
        w3.dst::text
    )
from webgraph1 w1
    inner JOIN webgraph1 w2 on w1.dst = w2.src
    inner JOIN webgraph1 w3 on w2.dst = w3.src
where w1.src != w2.dst;
-- Use Case D
WITH RECURSIVE n_nops (src, dst, length, path) AS (
    SELECT w.src,
        w.dst,
        1 AS length,
        w.src || '-' || w.dst AS path
    FROM webgraph1 w
    UNION
    SELECT n.src,
        w.dst,
        (n.length + 1),
        n.path || '-' || w.dst
    FROM webgraph1 w
        JOIN n_nops n ON n.dst = w.src
    WHERE n.path NOT LIKE '%' || w.dst || '%' -- Que el nuevo destino no haya estado presente
        AND length < 3
)
SELECT *
FROM n_nops
WHERE length = 3;