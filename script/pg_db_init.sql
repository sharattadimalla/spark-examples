
CREATE TABLE ITEM (
   id bigint primary key,
   name varchar(200),
   created_ts timestamp,
   updated_ts timestamp
);

INSERT INTO ITEM (id, name, created_ts, updated_ts) VALUES (1, 'item1', null , null);
INSERT INTO ITEM (id, name, created_ts, updated_ts) VALUES (2, 'item2', null, null);
INSERT INTO ITEM (id, name, created_ts, updated_ts) VALUES (3, 'item3', null, null);