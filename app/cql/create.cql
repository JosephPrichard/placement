CREATE KEYSPACE IF NOT EXISTS pks WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE TABLE IF NOT EXISTS pks.tiles (
        group_x int,
        group_y int,
        x int,
        y int,
        r tinyint,
        g tinyint,
        b tinyint,
        last_updated_ipaddress inet,
        last_updated_time timestamp,
        PRIMARY KEY ((group_x, group_y), x, y));
CREATE TABLE IF NOT EXISTS pks.placements (
		day bigint,
		x int,
		y int,
        r tinyint,
        g tinyint,
        b tinyint,
		ipaddress inet,
		placement_time timestamp,
		PRIMARY KEY (day, placement_time, ipaddress));