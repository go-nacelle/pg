package pgutil

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDrift_Extensions(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup:    []string{`CREATE EXTENSION hstore;`},
			Alter:    []string{`DROP EXTENSION hstore;`},
			Expected: []string{`CREATE EXTENSION IF NOT EXISTS "hstore" WITH SCHEMA "public";`},
		})
	})

	t.Run("drop", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup:    []string{},
			Alter:    []string{`CREATE EXTENSION pg_trgm;`},
			Expected: []string{`DROP EXTENSION IF EXISTS "pg_trgm";`},
		})
	})
}

func TestDrift_Enums(t *testing.T) {

	t.Run("create", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup:    []string{`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');`},
			Alter:    []string{`DROP TYPE mood;`},
			Expected: []string{`CREATE TYPE "public"."mood" AS ENUM ('sad', 'ok', 'happy');`},
		})

		t.Run("escaped values", func(t *testing.T) {
			testDrift(t, DriftTestCase{
				Setup:    []string{`CREATE TYPE spell_check AS ENUM ('there', 'their', 'they''re');`},
				Alter:    []string{`DROP TYPE spell_check;`},
				Expected: []string{`CREATE TYPE "public"."spell_check" AS ENUM ('there', 'their', 'they''re');`},
			})
		})
	})

	t.Run("drop", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup:    []string{},
			Alter:    []string{`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');`},
			Expected: []string{`DROP TYPE IF EXISTS "public"."mood";`},
		})
	})

	t.Run("alter", func(t *testing.T) {
		t.Run("missing labels", func(t *testing.T) {
			for _, testCase := range []struct {
				name            string
				expectedLabels  []string
				existingLabels  []string
				expectedQueries []string
			}{
				{
					name:           "missing at end",
					expectedLabels: []string{"foo", "bar", "baz", "bonk"},
					existingLabels: []string{"foo", "bar"},
					expectedQueries: []string{
						`ALTER TYPE "public"."mood" ADD VALUE 'baz' AFTER 'bar';`,
						`ALTER TYPE "public"."mood" ADD VALUE 'bonk' AFTER 'baz';`,
					},
				},
				{
					name:           "missing in middle",
					expectedLabels: []string{"foo", "bar", "baz", "bonk"},
					existingLabels: []string{"foo", "bonk"},
					expectedQueries: []string{
						`ALTER TYPE "public"."mood" ADD VALUE 'bar' AFTER 'foo';`,
						`ALTER TYPE "public"."mood" ADD VALUE 'baz' AFTER 'bar';`,
					},
				},
				{
					name:           "missing at beginning",
					expectedLabels: []string{"foo", "bar", "baz", "bonk"},
					existingLabels: []string{"baz", "bonk"},
					expectedQueries: []string{
						`ALTER TYPE "public"."mood" ADD VALUE 'foo' BEFORE 'baz';`,
						`ALTER TYPE "public"."mood" ADD VALUE 'bar' AFTER 'foo';`,
					},
				},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					// Prepare setup queries
					setupLabels := make([]string, len(testCase.expectedLabels))
					for i, label := range testCase.expectedLabels {
						setupLabels[i] = fmt.Sprintf("'%s'", label)
					}
					setupQuery := fmt.Sprintf("CREATE TYPE mood AS ENUM (%s);", strings.Join(setupLabels, ", "))

					// Prepare alter queries
					existingLabelsFormatted := make([]string, len(testCase.existingLabels))
					for i, label := range testCase.existingLabels {
						existingLabelsFormatted[i] = fmt.Sprintf("'%s'", label)
					}
					alterQuery := fmt.Sprintf(`
						DROP TYPE mood;
						CREATE TYPE mood AS ENUM (%s);
					`, strings.Join(existingLabelsFormatted, ", "))

					// Execute testDrift with the new struct
					testDrift(t, DriftTestCase{
						Setup:    []string{setupQuery},
						Alter:    []string{alterQuery},
						Expected: testCase.expectedQueries,
					})
				})
			}

			t.Run("escaped values", func(t *testing.T) {
				testDrift(t, DriftTestCase{
					Setup: []string{
						`CREATE TYPE spell_check AS ENUM ('they''re', 'there', 'their', 'whose', 'who''s');`,
					},
					Alter: []string{
						`DROP TYPE spell_check;`,
						`CREATE TYPE spell_check AS ENUM ('they''re', 'their', 'whose');`,
					},
					Expected: []string{
						`ALTER TYPE "public"."spell_check" ADD VALUE 'there' AFTER 'they''re';`,
						`ALTER TYPE "public"."spell_check" ADD VALUE 'who''s' AFTER 'whose';`,
					},
				})
			})
		})

		t.Run("non-repairable labels", func(t *testing.T) {
			testDrift(t, DriftTestCase{
				Setup: []string{
					`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');`,
				},
				Alter: []string{
					`DROP TYPE mood;`,
					`CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok', 'gleeful');`,
				},
				Expected: []string{
					`ALTER TYPE "public"."mood" RENAME TO "mood_bak";`,
					`CREATE TYPE "public"."mood" AS ENUM ('sad', 'ok', 'happy');`,
					`DROP TYPE "public"."mood_bak";`,
				},
			})
		})

		t.Run("updates column types", func(t *testing.T) {
			testDrift(t, DriftTestCase{
				Setup: []string{
					`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');`,
					`CREATE TABLE t (m mood);`,
				},
				Alter: []string{
					`DROP TABLE t;`,
					`DROP TYPE mood;`,
					`CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok', 'gleeful');`,
					`CREATE TABLE t (m mood);`,
				},
				Expected: []string{
					`ALTER TYPE "public"."mood" RENAME TO "mood_bak";`,
					`CREATE TYPE "public"."mood" AS ENUM ('sad', 'ok', 'happy');`,
					`ALTER TABLE "public"."t" ALTER COLUMN "m" TYPE "public"."mood" USING ("m"::text::"public"."mood");`,
					`DROP TYPE "public"."mood_bak";`,
				},
			})
		})

		t.Run("updates column defaults", func(t *testing.T) {
			testDrift(t, DriftTestCase{
				Setup: []string{
					`CREATE SCHEMA s;`,
					`CREATE TYPE s.mood AS ENUM ('sad', 'ok', 'happy');`,
					`CREATE TABLE s.t (m s.mood DEFAULT 'sad');`,
				},
				Alter: []string{
					`DROP TABLE s.t;`,
					`DROP TYPE s.mood;`,
					`CREATE TYPE s.mood AS ENUM ('happy', 'sad', 'ok', 'gleeful');`,
					`CREATE TABLE s.t (m s.mood DEFAULT 'sad');`,
				},
				Expected: []string{
					`ALTER TYPE "s"."mood" RENAME TO "mood_bak";`,
					`CREATE TYPE "s"."mood" AS ENUM ('sad', 'ok', 'happy');`,
					`ALTER TABLE "s"."t" ALTER COLUMN "m" DROP DEFAULT, ALTER COLUMN "m" TYPE "s"."mood" USING ("m"::text::"s"."mood"), ALTER COLUMN "m" SET DEFAULT 'sad'::s.mood;`,
					`DROP TYPE "s"."mood_bak";`,
				},
			})
		})

		t.Run("temporarily drops dependent views", func(t *testing.T) {
			testDrift(t, DriftTestCase{
				Setup: []string{
					`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');`,
					`CREATE TABLE t (m mood);`,
					`CREATE VIEW v1 AS SELECT m FROM t;`,
					`CREATE VIEW v2 AS SELECT m FROM v1;`,
				},
				Alter: []string{
					`DROP VIEW v2;`,
					`DROP VIEW v1;`,
					`DROP TABLE t;`,
					`DROP TYPE mood;`,
					`CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok', 'gleeful');`,
					`CREATE TABLE t (m mood);`,
					`CREATE VIEW v1 AS SELECT m FROM t;`,
					`CREATE VIEW v2 AS SELECT m FROM v1;`,
				},
				Expected: []string{
					`DROP VIEW IF EXISTS "public"."v2";`,
					`DROP VIEW IF EXISTS "public"."v1";`,
					`ALTER TYPE "public"."mood" RENAME TO "mood_bak";`,
					`CREATE TYPE "public"."mood" AS ENUM ('sad', 'ok', 'happy');`,
					`ALTER TABLE "public"."t" ALTER COLUMN "m" TYPE "public"."mood" USING ("m"::text::"public"."mood");`,
					`DROP TYPE "public"."mood_bak";`,
					`CREATE OR REPLACE VIEW "public"."v1" AS SELECT m
 FROM t;`,
					`CREATE OR REPLACE VIEW "public"."v2" AS SELECT m
 FROM v1;`,
				},
			})
		})
	})
}

var postgresAddFunctionDefinition = `CREATE OR REPLACE FUNCTION public.add(integer, integer)
 RETURNS integer
 LANGUAGE sql
AS $function$SELECT $1 + $2;$function$
;`

func TestDrift_Functions(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup:    []string{`CREATE FUNCTION add(integer, integer) RETURNS integer AS 'SELECT $1 + $2;' LANGUAGE SQL;`},
			Alter:    []string{`DROP FUNCTION add(integer, integer);`},
			Expected: []string{postgresAddFunctionDefinition},
		})
	})

	t.Run("drop", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup:    []string{},
			Alter:    []string{`CREATE FUNCTION add(integer, integer) RETURNS integer AS 'SELECT $1 + $2;' LANGUAGE SQL;`},
			Expected: []string{`DROP FUNCTION IF EXISTS "public"."add"(int4, int4);`},
		})
	})

	t.Run("alter", func(t *testing.T) {
		t.Run("mismatched definition", func(t *testing.T) {
			testDrift(t, DriftTestCase{
				Setup:    []string{`CREATE FUNCTION add(integer, integer) RETURNS integer AS 'SELECT $1 + $2;' LANGUAGE SQL;`},
				Alter:    []string{`CREATE OR REPLACE FUNCTION add(integer, integer) RETURNS integer AS 'SELECT $1 - $2;' LANGUAGE SQL;`},
				Expected: []string{postgresAddFunctionDefinition},
			})
		})

		t.Run("preserves functions with differing argument types", func(t *testing.T) {
			testDrift(t, DriftTestCase{
				Setup:    []string{`CREATE FUNCTION add(integer, integer) RETURNS integer AS 'SELECT $1 + $2;' LANGUAGE SQL;`},
				Alter:    []string{`CREATE FUNCTION add(integer, integer, integer) RETURNS integer AS 'SELECT $1 + $2 + $3;' LANGUAGE SQL;`},
				Expected: []string{`DROP FUNCTION IF EXISTS "public"."add"(int4, int4, int4);`},
			})
		})
	})
}

func TestDrift_Tables(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup: []string{
				`
					CREATE TABLE my_table (
						id INTEGER PRIMARY KEY,
						name TEXT
					);
				`,
			},
			Alter: []string{
				`DROP TABLE my_table;`,
			},
			Expected: []string{
				`CREATE TABLE IF NOT EXISTS "public"."my_table"();`,
				`ALTER TABLE "public"."my_table" ADD COLUMN IF NOT EXISTS "id" integer NOT NULL;`,
				`ALTER TABLE "public"."my_table" ADD COLUMN IF NOT EXISTS "name" text;`,
				`ALTER TABLE "public"."my_table" ADD CONSTRAINT "my_table_pkey" PRIMARY KEY (id);`,
			},
		})
	})

	t.Run("drop", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup: []string{},
			Alter: []string{
				`
					CREATE TABLE my_table (
						id INTEGER PRIMARY KEY,
						name TEXT
					);
				`,
			},
			Expected: []string{
				`ALTER TABLE "public"."my_table" DROP CONSTRAINT IF EXISTS "my_table_pkey";`,
				`ALTER TABLE "public"."my_table" DROP COLUMN IF EXISTS "id";`,
				`ALTER TABLE "public"."my_table" DROP COLUMN IF EXISTS "name";`,
				`DROP TABLE IF EXISTS "public"."my_table";`,
			},
		})
	})

	t.Run("alter", func(t *testing.T) {
		t.Run("columns", func(t *testing.T) {
			t.Run("missing", func(t *testing.T) {
				testDrift(t, DriftTestCase{
					Setup: []string{
						`
							CREATE TABLE my_table (
								id INTEGER PRIMARY KEY,
								name TEXT
							);
						`,
					},
					Alter: []string{
						`ALTER TABLE my_table DROP COLUMN name;`,
					},
					Expected: []string{
						`ALTER TABLE "public"."my_table" ADD COLUMN IF NOT EXISTS "name" text;`,
					},
				})
			})

			t.Run("extra", func(t *testing.T) {
				testDrift(t, DriftTestCase{
					Setup: []string{
						`
							CREATE TABLE my_table (
								id INTEGER PRIMARY KEY,
								name TEXT
							);
						`,
					},
					Alter: []string{
						`ALTER TABLE my_table ADD COLUMN age INTEGER;`,
					},
					Expected: []string{
						`ALTER TABLE "public"."my_table" DROP COLUMN IF EXISTS "age";`,
					},
				})
			})

			t.Run("mismatched type", func(t *testing.T) {
				testDrift(t, DriftTestCase{
					Setup: []string{
						`
							CREATE TABLE my_table (
								id INTEGER PRIMARY KEY,
								name TEXT
							);
						`,
					},
					Alter: []string{
						`ALTER TABLE my_table ALTER COLUMN name TYPE VARCHAR(255);`,
					},
					Expected: []string{
						`ALTER TABLE "public"."my_table" ALTER COLUMN "name" SET DATA TYPE text;`,
					},
				})
			})

			t.Run("mismatched default", func(t *testing.T) {
				t.Run("add default", func(t *testing.T) {
					testDrift(t, DriftTestCase{
						Setup: []string{
							`
								CREATE TABLE my_table (
									id INTEGER PRIMARY KEY,
									name TEXT
								);
							`,
						},
						Alter: []string{
							`ALTER TABLE my_table ALTER COLUMN name SET DEFAULT 'foo';`,
						},
						Expected: []string{
							`ALTER TABLE "public"."my_table" ALTER COLUMN "name" DROP DEFAULT;`,
						},
					})

					t.Run("drop default", func(t *testing.T) {
						testDrift(t, DriftTestCase{
							Setup: []string{
								`
									CREATE TABLE my_table (
										id INTEGER PRIMARY KEY,
										name TEXT DEFAULT 'foo'
									);
								`,
							},
							Alter: []string{
								`ALTER TABLE my_table ALTER COLUMN name DROP DEFAULT;`,
							},
							Expected: []string{
								`ALTER TABLE "public"."my_table" ALTER COLUMN "name" SET DEFAULT 'foo'::text;`,
							},
						})
					})

					t.Run("change default", func(t *testing.T) {
						testDrift(t, DriftTestCase{
							Setup: []string{
								`
									CREATE TABLE my_table (
										id INTEGER PRIMARY KEY,
										name TEXT DEFAULT 'foo'
									);
								`,
							},
							Alter: []string{
								`ALTER TABLE my_table ALTER COLUMN name SET DEFAULT 'bar';`,
							},
							Expected: []string{
								`ALTER TABLE "public"."my_table" ALTER COLUMN "name" SET DEFAULT 'foo'::text;`,
							},
						})
					})
				})

				t.Run("mismatched nullability", func(t *testing.T) {
					t.Run("add not null", func(t *testing.T) {
						testDrift(t, DriftTestCase{
							Setup: []string{
								`
									CREATE TABLE my_table (
										id INTEGER PRIMARY KEY,
										name TEXT
									);
								`,
							},
							Alter: []string{
								`ALTER TABLE my_table ALTER COLUMN name SET NOT NULL;`,
							},
							Expected: []string{
								`ALTER TABLE "public"."my_table" ALTER COLUMN "name" DROP NOT NULL;`,
							},
						})
					})

					t.Run("drop not null`", func(t *testing.T) {
						testDrift(t, DriftTestCase{
							Setup: []string{
								`
									CREATE TABLE my_table (
										id INTEGER PRIMARY KEY,
										name TEXT NOT NULL
									);
								`,
							},
							Alter: []string{
								`ALTER TABLE my_table ALTER COLUMN name DROP NOT NULL;`,
							},
							Expected: []string{
								`ALTER TABLE "public"."my_table" ALTER COLUMN "name" SET NOT NULL;`,
							},
						})
					})
				})

				t.Run("multiple changes", func(t *testing.T) {
					testDrift(t, DriftTestCase{
						Setup: []string{
							`
								CREATE TABLE my_table (
									id INTEGER PRIMARY KEY,
									name TEXT DEFAULT 'foo'
								);
							`,
						},
						Alter: []string{
							`ALTER TABLE my_table ALTER COLUMN name SET DEFAULT 'bar';`,
							`ALTER TABLE my_table ALTER COLUMN name SET NOT NULL;`,
						},
						Expected: []string{
							`ALTER TABLE "public"."my_table" ALTER COLUMN "name" SET DEFAULT 'foo'::text;`,
							`ALTER TABLE "public"."my_table" ALTER COLUMN "name" DROP NOT NULL;`,
						},
					})
				})
			})

			t.Run("constraint", func(t *testing.T) {
				t.Run("missing", func(t *testing.T) {
					testDrift(t, DriftTestCase{
						Setup: []string{
							`
							CREATE TABLE my_table (
								id INTEGER PRIMARY KEY,
								name TEXT UNIQUE
							);
						`,
						},
						Alter: []string{
							`ALTER TABLE my_table DROP CONSTRAINT my_table_name_key;`,
						},
						Expected: []string{
							`ALTER TABLE "public"."my_table" ADD CONSTRAINT "my_table_name_key" UNIQUE (name);`,
						},
					})
				})

				t.Run("extra", func(t *testing.T) {
					testDrift(t, DriftTestCase{
						Setup: []string{
							`
							CREATE TABLE my_table (
								id INTEGER PRIMARY KEY,
								name TEXT
							);
						`,
						},
						Alter: []string{
							`ALTER TABLE my_table ADD CONSTRAINT my_table_name_key UNIQUE (name);`,
						},
						Expected: []string{
							`ALTER TABLE "public"."my_table" DROP CONSTRAINT IF EXISTS "my_table_name_key";`,
						},
					})
				})

				t.Run("alter", func(t *testing.T) {
					testDrift(t, DriftTestCase{
						Setup: []string{
							`
						CREATE TABLE my_table (
							id INTEGER PRIMARY KEY,
							name TEXT
						);
					`,
						},
						Alter: []string{
							`ALTER TABLE my_table DROP CONSTRAINT my_table_pkey;`,
							`ALTER TABLE my_table ADD CONSTRAINT my_table_pkey UNIQUE (name);`,
						},
						Expected: []string{
							`ALTER TABLE "public"."my_table" DROP CONSTRAINT IF EXISTS "my_table_pkey";`,
							`ALTER TABLE "public"."my_table" ADD CONSTRAINT "my_table_pkey" PRIMARY KEY (id);`,
						},
					})
				})
			})

			t.Run("index", func(t *testing.T) {
				t.Run("missing", func(t *testing.T) {
					testDrift(t, DriftTestCase{
						Setup: []string{
							`
							CREATE SCHEMA s;
							CREATE TABLE s.my_table (
								id INTEGER PRIMARY KEY,
								name TEXT
							);
						`,
							`CREATE INDEX my_table_name_idx ON s.my_table (name);`,
						},
						Alter: []string{
							`DROP INDEX s.my_table_name_idx;`,
						},
						Expected: []string{
							`CREATE INDEX my_table_name_idx ON s.my_table USING btree (name);`,
						},
					})
				})

				t.Run("extra", func(t *testing.T) {
					testDrift(t, DriftTestCase{
						Setup: []string{
							`
							CREATE SCHEMA s;
							CREATE TABLE s.my_table (
								id INTEGER PRIMARY KEY,
								name TEXT
							);
						`,
						},
						Alter: []string{
							`CREATE INDEX my_table_name_idx ON s.my_table (name);`,
						},
						Expected: []string{
							`DROP INDEX IF EXISTS "s"."my_table_name_idx";`,
						},
					})
				})

				t.Run("alter", func(t *testing.T) {
					testDrift(t, DriftTestCase{
						Setup: []string{
							`
							CREATE SCHEMA s;
							CREATE TABLE s.my_table (
								id INTEGER PRIMARY KEY,
								name TEXT
							);
						`,
							`CREATE INDEX my_table_name_idx ON s.my_table (name);`,
						},
						Alter: []string{
							`DROP INDEX s.my_table_name_idx;`,
							`CREATE INDEX my_table_name_idx ON s.my_table (name DESC);`,
						},
						Expected: []string{
							`DROP INDEX IF EXISTS "s"."my_table_name_idx";`,
							`CREATE INDEX my_table_name_idx ON s.my_table USING btree (name);`,
						},
					})
				})
			})
		})
	})
}

func TestDrift_Sequences(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup:    []string{`CREATE SEQUENCE my_seq AS bigint;`},
			Alter:    []string{`DROP SEQUENCE my_seq;`},
			Expected: []string{`CREATE SEQUENCE IF NOT EXISTS "public"."my_seq" AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 NO CYCLE;`},
		})
	})

	t.Run("drop", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup:    []string{},
			Alter:    []string{`CREATE SEQUENCE my_seq;`},
			Expected: []string{`DROP SEQUENCE IF EXISTS "public"."my_seq";`},
		})
	})

	t.Run("alter", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup: []string{
				`CREATE SEQUENCE my_seq AS bigint INCREMENT BY 2 MINVALUE 2 MAXVALUE 12000 START WITH 2 CYCLE;`,
			},
			Alter: []string{
				`DROP SEQUENCE my_seq;`,
				`CREATE SEQUENCE my_seq AS int INCREMENT BY 1 MINVALUE 1 MAXVALUE 24000 START WITH 1 NO CYCLE;`,
				`SELECT setval('my_seq', 43, true);`,
			},
			Expected: []string{
				`ALTER SEQUENCE IF EXISTS "public"."my_seq" AS bigint INCREMENT BY 2 MINVALUE 2 MAXVALUE 12000 START WITH 2 CYCLE;`,
			},
		})
	})
}

func TestDrift_Views(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup:    []string{`CREATE VIEW my_view AS SELECT 1 AS one;`},
			Alter:    []string{`DROP VIEW my_view;`},
			Expected: []string{`CREATE OR REPLACE VIEW "public"."my_view" AS SELECT 1 AS one;`},
		})
	})

	t.Run("drop", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup:    []string{},
			Alter:    []string{`CREATE VIEW my_view AS SELECT 1 AS one;`},
			Expected: []string{`DROP VIEW IF EXISTS "public"."my_view";`},
		})
	})

	t.Run("alter", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup: []string{
				`CREATE VIEW my_view AS SELECT 1 AS one;`,
			},
			Alter: []string{
				`DROP VIEW my_view;`,
				`CREATE OR REPLACE VIEW my_view AS SELECT 2 AS two;`,
			},
			Expected: []string{
				`DROP VIEW IF EXISTS "public"."my_view";`,
				`CREATE OR REPLACE VIEW "public"."my_view" AS SELECT 1 AS one;`,
			},
		})
	})

	t.Run("dependency closure", func(t *testing.T) {
		t.Run("create", func(t *testing.T) {
			testDrift(t, DriftTestCase{
				Setup: []string{
					`CREATE TABLE t (x int);`,
					`CREATE VIEW v_foo AS SELECT * FROM t;`,
					`CREATE VIEW v_bar AS SELECT * FROM v_foo;`,
					`CREATE VIEW v_baz AS SELECT * FROM t UNION SELECT * FROM v_foo;`,
					`CREATE VIEW v_bonk AS SELECT * FROM t UNION SELECT * FROM v_bar;`,
					`CREATE VIEW v_quux AS SELECT * FROM v_foo UNION SELECT * FROM v_bar;`,
					`CREATE VIEW v_one AS SELECT 1 AS one;`,
					`CREATE VIEW v_two AS SELECT 2 AS two;`,
				},
				Alter: []string{
					`DROP VIEW v_two;`,
					`DROP VIEW v_one;`,
					`DROP VIEW v_quux;`,
					`DROP VIEW v_bonk;`,
					`DROP VIEW v_baz;`,
					`DROP VIEW v_bar;`,
					`DROP VIEW v_foo;`,
				},
				Expected: []string{
					`CREATE OR REPLACE VIEW "public"."v_foo" AS SELECT x
 FROM t;`,
					`CREATE OR REPLACE VIEW "public"."v_bar" AS SELECT x
 FROM v_foo;`,
					`CREATE OR REPLACE VIEW "public"."v_baz" AS SELECT t.x
   FROM t
UNION
 SELECT v_foo.x
   FROM v_foo;`,
					`CREATE OR REPLACE VIEW "public"."v_bonk" AS SELECT t.x
   FROM t
UNION
 SELECT v_bar.x
   FROM v_bar;`,
					`CREATE OR REPLACE VIEW "public"."v_one" AS SELECT 1 AS one;`,
					`CREATE OR REPLACE VIEW "public"."v_quux" AS SELECT v_foo.x
   FROM v_foo
UNION
 SELECT v_bar.x
   FROM v_bar;`,
					`CREATE OR REPLACE VIEW "public"."v_two" AS SELECT 2 AS two;`,
				},
			})
		})

		t.Run("drop", func(t *testing.T) {
			testDrift(t, DriftTestCase{
				Setup: []string{
					`CREATE TABLE t (x int);`,
				},
				Alter: []string{
					`CREATE VIEW v_foo AS SELECT * FROM t;`,
					`CREATE VIEW v_bar AS SELECT * FROM v_foo;`,
					`CREATE VIEW v_baz AS SELECT * FROM t UNION SELECT * FROM v_foo;`,
					`CREATE VIEW v_bonk AS SELECT * FROM t UNION SELECT * FROM v_bar;`,
					`CREATE VIEW v_quux AS SELECT * FROM v_foo UNION SELECT * FROM v_bar;`,
					`CREATE VIEW v_one AS SELECT 1 AS one;`,
					`CREATE VIEW v_two AS SELECT 2 AS two;`,
				},
				Expected: []string{
					`DROP VIEW IF EXISTS "public"."v_baz";`,
					`DROP VIEW IF EXISTS "public"."v_bonk";`,
					`DROP VIEW IF EXISTS "public"."v_one";`,
					`DROP VIEW IF EXISTS "public"."v_quux";`,
					`DROP VIEW IF EXISTS "public"."v_bar";`,
					`DROP VIEW IF EXISTS "public"."v_foo";`,
					`DROP VIEW IF EXISTS "public"."v_two";`,
				},
			})
		})
	})
}

func TestDrift_Triggers(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup: []string{
				`CREATE SCHEMA a;`,
				`CREATE SCHEMA b;`,
				`CREATE TABLE a.t (x int);`,

				`CREATE FUNCTION b.f() RETURNS TRIGGER AS $$
				BEGIN RETURN NEW; END;
				$$ LANGUAGE plpgsql;`,

				`CREATE TRIGGER "t-insert" BEFORE INSERT ON a.t FOR EACH ROW EXECUTE FUNCTION b.f();`,
			},
			Alter: []string{
				`DROP TRIGGER "t-insert" ON a.t;`,
			},
			Expected: []string{
				`CREATE TRIGGER "t-insert" BEFORE INSERT ON a.t FOR EACH ROW EXECUTE FUNCTION b.f();`,
			},
		})
	})

	t.Run("drop", func(t *testing.T) {
		testDrift(t, DriftTestCase{
			Setup: []string{
				`CREATE SCHEMA a;`,
				`CREATE SCHEMA b;`,
				`CREATE TABLE a.t (x int);`,
				`CREATE FUNCTION b.f() RETURNS TRIGGER AS $$
				BEGIN RETURN NEW; END;
				$$ LANGUAGE plpgsql;`,
			},
			Alter: []string{
				`CREATE TRIGGER "t-insert" BEFORE INSERT ON a.t FOR EACH ROW EXECUTE FUNCTION b.f();`,
			},
			Expected: []string{
				`DROP TRIGGER IF EXISTS "t-insert" ON "a"."t";`,
			},
		})
	})
}

//
//

type DriftTestCase struct {
	Setup    []string
	Alter    []string
	Expected []string
}

func testDrift(t *testing.T, testCase DriftTestCase) {
	t.Helper()
	db := NewTestDB(t)
	ctx := context.Background()

	// Execute all setup queries
	for _, query := range testCase.Setup {
		require.NoError(t, db.Exec(ctx, RawQuery(query)), "query=%q", query)
	}

	// Describe the initial schema
	before, err := DescribeSchema(ctx, db)
	if err != nil {
		t.Fatalf("Failed to describe schema: %v", err)
	}

	// Execute all alter queries
	for _, query := range testCase.Alter {
		require.NoError(t, db.Exec(ctx, RawQuery(query)), "query=%q", query)
	}

	// Describe the altered schema
	after, err := DescribeSchema(ctx, db)
	require.NoError(t, err)

	// Compare schemas and assert expected drift
	require.Equal(t, testCase.Expected, Compare(before, after))

	// Apply the expected repair queries
	for _, query := range testCase.Expected {
		require.NoError(t, db.Exec(ctx, RawQuery(query)), "query=%q", query)
	}

	// Verify that the drift has been repaired
	repaired, err := DescribeSchema(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, Compare(before, repaired))
}
