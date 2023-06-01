# replace the sgdXXX with your correct table name

```bash
find . -name '*.ddl' -exec sed -i s/sgdXXX/sgd1/g {} \; 
```

# use psql or your favorite client to create the indexes

```bash
for i in *.ddl; do psql postgres://user:password@1.2.3.4/dbname -f $i; done
```

# to speed things up, you could run each ddl file in parallel, from a different shell.
# concurrent operations on different tables will NOT get in the way of each other,
# but they may use more RAM on the postgresql server, beware of OOMKills

```
# shell 1
psql postgres://user:password@1.2.3.4/dbname -f bundle.ddl

# shell 2
psql postgres://user:password@1.2.3.4/dbname -f pair.ddl

# etc.
```
