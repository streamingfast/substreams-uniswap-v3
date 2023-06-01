create index if not exists bundle_id_block_range_excl on "sgdXXX"."bundle" using gist (id, block_range);
create index if not exists brin_bundle on "sgdXXX"."bundle" using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index if not exists bundle_block_range_closed on "sgdXXX"."bundle"(coalesce(upper(block_range), 2147483647)) where coalesce(upper(block_range), 2147483647) < 2147483647;
create index if not exists attr_1_0_bundle_id on "sgdXXX"."bundle" using btree("id");
create index if not exists attr_1_1_bundle_eth_price_usd on "sgdXXX"."bundle" using btree("eth_price_usd");
