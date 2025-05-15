use crate::db::Type;

/// Key-value pair that can be used in [`StmtBuilder`].
#[derive(PartialEq, Eq, Clone, Copy, Hash, Debug)]
pub struct KV<'a> {
    pub key: &'a str,
    pub val: &'a str,
}

/// Placeholder for binding a parameter.
pub const PLACEHOLDER: &str = "?";

const PG_PLACEHOLDER_BEGIN_IDX: i32 = 1;

/// SQL statement builder.
///
/// This builder will use string replacement to build SQL statements,
/// so please make sure the values used here, for example the table name, column names and values,
/// are safe and won't lead to SQL injection.
///
/// If you want to build prepared statements, or use SDKs like sqlx to parse the output statements,
/// you can use placeholders to allow for binding parameters to the statements.
/// Binding parameters is safe and won't lead to SQL injection.
///
/// The placeholders used in different databases are listed as follows:
///
///   - MySQL: `?`
///   - PostgreSQL: `$N`, where N is the 1-based positional argument index.
///   - SQLite: `?`
///
/// If the given database type is PostgreSQL, and the given value is `?`,
/// this builder will automatically converts `?` to `$N` based placeholders.
///
/// Consider using [`PLACEHOLDER`] to represent a placeholder.
pub struct StmtBuilder {
    tbl: String,
    typ: Type,
}

impl StmtBuilder {
    /// Creates a new [`StmtBuilder`], where `tbl` is the table name and `typ` is the database type.
    pub fn new(tbl: String, typ: Type) -> StmtBuilder {
        StmtBuilder { tbl, typ }
    }

    /// Gets table name.
    pub fn get_tbl(&self) -> &String {
        &self.tbl
    }

    /// Gets database type.
    pub fn get_typ(&self) -> &Type {
        &self.typ
    }

    fn escape_col(&self, col: &str) -> String {
        if col == "*" {
            return col.to_string();
        }
        match self.typ {
            Type::MySQL => format!("`{}`", col),
            Type::PostgreSQL | Type::SQLite => format!("\"{}\"", col),
        }
    }

    fn convert_placeholder(&self, begin_idx: &mut i32, val: &str) -> String {
        match self.typ {
            Type::MySQL | Type::SQLite => val.to_string(),
            Type::PostgreSQL => {
                if val == PLACEHOLDER {
                    *begin_idx += 1;
                    format!("${}", *begin_idx - 1)
                } else {
                    val.to_string()
                }
            }
        }
    }

    fn build_conds(&self, begin_idx: &mut i32, conds: &[KV]) -> String {
        if conds.is_empty() {
            String::new()
        } else {
            format!(
                " WHERE {}",
                conds
                    .iter()
                    .map(|kv| format!(
                        "{} = {}",
                        kv.key,
                        self.convert_placeholder(begin_idx, kv.val)
                    ))
                    .collect::<Vec<String>>()
                    .join(" AND ")
            )
        }
    }

    /// Builds a SQL statement that performs insert operation.
    ///
    /// # Arguments
    ///
    /// * `cols` - The column names and values. If it's empty, an empty string will be returned.
    ///
    /// # Returns
    ///
    /// * The SQL statement.
    ///
    /// # Examples
    ///
    /// ```
    /// use sainnhe_common::db::{KV, PLACEHOLDER, StmtBuilder, Type};
    ///
    /// let sb = StmtBuilder::new(String::from("my_tbl"), Type::MySQL);
    /// let cols = vec![
    ///     KV {
    ///         key: "username",
    ///         val: PLACEHOLDER,
    ///     },
    ///     KV {
    ///         key: "nickname",
    ///         val: "'foo'",
    ///     },
    ///     KV {
    ///         key: "create_at",
    ///         val: "NOW()",
    ///     },
    /// ];
    ///
    /// let stmt = sb.build_insert_stmt(&cols);
    /// let expected_stmt =
    ///     "INSERT INTO my_tbl (`username`, `nickname`, `create_at`) VALUES (?, 'foo', NOW())";
    ///
    /// assert_eq!(stmt, expected_stmt);
    /// ```
    pub fn build_insert_stmt(&self, cols: &[KV]) -> String {
        if cols.is_empty() {
            return String::new();
        }
        let mut begin_idx = PG_PLACEHOLDER_BEGIN_IDX;
        let (keys, vals): (Vec<String>, Vec<String>) = cols
            .iter()
            .map(|kv| {
                (
                    self.escape_col(kv.key),
                    self.convert_placeholder(&mut begin_idx, kv.val),
                )
            })
            .unzip();
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.tbl,
            keys.join(", "),
            vals.join(", ")
        )
    }

    /// Builds a SQL statement that performs query operation.
    ///
    /// # Arguments
    ///
    /// * `cols` - The selected columns. If it's empty, `["*"]` will be used.
    /// * `conds` - The equal conditions.
    ///
    /// # Returns
    ///
    /// * The SQL statement.
    ///
    /// # Examples
    ///
    /// ```
    /// use sainnhe_common::db::{KV, PLACEHOLDER, StmtBuilder, Type};
    ///
    /// let sb = StmtBuilder::new(String::from("my_tbl"), Type::PostgreSQL);
    /// let cols = vec![String::from("username"), String::from("nickname")];
    /// let conds = vec![
    ///     KV {
    ///         key: "age",
    ///         val: PLACEHOLDER,
    ///     },
    ///     KV {
    ///         key: "gender",
    ///         val: PLACEHOLDER,
    ///     },
    /// ];
    ///
    /// let stmt = sb.build_query_stmt(&cols, &conds);
    /// let expected_stmt = "SELECT \"username\", \"nickname\" FROM my_tbl WHERE age = $1 AND gender = $2";
    ///
    /// assert_eq!(stmt, expected_stmt);
    /// ```
    pub fn build_query_stmt(&self, cols: &[String], conds: &[KV]) -> String {
        let cols_str = if cols.is_empty() {
            String::from("*")
        } else {
            cols.iter()
                .map(|col| self.escape_col(col))
                .collect::<Vec<String>>()
                .join(", ")
        };
        let mut begin_idx = PG_PLACEHOLDER_BEGIN_IDX;
        format!(
            "SELECT {} FROM {}{}",
            cols_str,
            self.tbl,
            self.build_conds(&mut begin_idx, conds)
        )
    }

    /// Builds a SQL statement that performs update operation.
    ///
    /// # Arguments
    ///
    /// * `cols` - The column names and values. If it's empty, an empty string will be returned.
    /// * `conds` - The equal conditions.
    ///
    /// # Returns
    ///
    /// * The SQL statement.
    ///
    /// # Examples
    ///
    /// ```
    /// use sainnhe_common::db::{KV, PLACEHOLDER, StmtBuilder, Type};
    ///
    /// let sb = StmtBuilder::new(String::from("my_tbl"), Type::PostgreSQL);
    /// let cols = vec![
    ///     KV {
    ///         key: "username",
    ///         val: PLACEHOLDER,
    ///     },
    ///     KV {
    ///         key: "nickname",
    ///         val: PLACEHOLDER,
    ///     },
    ///     KV {
    ///         key: "update_at",
    ///         val: "NOW()",
    ///     },
    /// ];
    /// let conds = vec![
    ///     KV {
    ///         key: "age",
    ///         val: PLACEHOLDER,
    ///     },
    ///     KV {
    ///         key: "gender",
    ///         val: "'male'",
    ///     },
    /// ];
    ///
    /// let stmt = sb.build_update_stmt(&cols, &conds);
    /// let expected_stmt = "UPDATE my_tbl SET \"username\" = $1, \"nickname\" = $2, \"update_at\" = NOW() WHERE age = $3 AND gender = 'male'";
    ///
    /// assert_eq!(stmt, expected_stmt);
    /// ```
    pub fn build_update_stmt(&self, cols: &[KV], conds: &[KV]) -> String {
        if cols.is_empty() {
            return String::new();
        }
        let mut begin_idx = PG_PLACEHOLDER_BEGIN_IDX;
        format!(
            "UPDATE {} SET {}{}",
            self.tbl,
            cols.iter()
                .map(|kv| format!(
                    "{} = {}",
                    self.escape_col(kv.key),
                    self.convert_placeholder(&mut begin_idx, kv.val)
                ))
                .collect::<Vec<String>>()
                .join(", "),
            self.build_conds(&mut begin_idx, conds)
        )
    }

    /// Builds a SQL statement that performs delete operation.
    ///
    /// # Arguments
    ///
    /// * `conds` - The equal conditions.
    ///
    /// # Returns
    ///
    /// * The SQL statement.
    ///
    /// # Examples
    ///
    /// ```
    /// use sainnhe_common::db::{KV, PLACEHOLDER, StmtBuilder, Type};
    ///
    /// let sb = StmtBuilder::new(String::from("my_tbl"), Type::SQLite);
    /// let conds = vec![
    ///     KV {
    ///         key: "username",
    ///         val: PLACEHOLDER,
    ///     },
    ///     KV {
    ///         key: "age",
    ///         val: "25",
    ///     },
    /// ];
    ///
    /// let stmt = sb.build_delete_stmt(&conds);
    /// let expected_stmt = "DELETE FROM my_tbl WHERE username = ? AND age = 25";
    ///
    /// assert_eq!(stmt, expected_stmt);
    /// ```
    pub fn build_delete_stmt(&self, conds: &[KV]) -> String {
        let mut begin_idx = PG_PLACEHOLDER_BEGIN_IDX;
        format!(
            "DELETE FROM {}{}",
            self.tbl,
            self.build_conds(&mut begin_idx, conds)
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{PLACEHOLDER, Type};

    use super::{KV, StmtBuilder};

    static TABLE: &str = "my_tbl";

    #[test]
    fn test_getter() {
        let sb = StmtBuilder::new(String::from(TABLE), Type::MySQL);
        assert_eq!(TABLE, sb.get_tbl());
        assert!(match sb.get_typ() {
            Type::MySQL => true,
            _ => false,
        });
    }

    #[test]
    fn test_build_insert_stmt() {
        struct TC<'a> {
            cols: &'a [KV<'a>],
            want_mysql: &'a str,
            want_postgresql: &'a str,
            want_sqlite: &'a str,
        }

        let cols1 = vec![KV {
            key: "name",
            val: "'product'",
        }];
        let cols2 = vec![
            KV {
                key: "email",
                val: PLACEHOLDER,
            },
            KV {
                key: "age",
                val: "20",
            },
            KV {
                key: "username",
                val: PLACEHOLDER,
            },
        ];
        let cols3: Vec<KV<'_>> = Vec::new();

        let test_cases = vec![
            // Single column
            TC {
                cols: &cols1,
                want_mysql: "INSERT INTO my_tbl (`name`) VALUES ('product')",
                want_postgresql: "INSERT INTO my_tbl (\"name\") VALUES ('product')",
                want_sqlite: "INSERT INTO my_tbl (\"name\") VALUES ('product')",
            },
            // Multiple columns
            TC {
                cols: &cols2,
                want_mysql: "INSERT INTO my_tbl (`email`, `age`, `username`) VALUES (?, 20, ?)",
                want_postgresql: "INSERT INTO my_tbl (\"email\", \"age\", \"username\") VALUES ($1, 20, $2)",
                want_sqlite: "INSERT INTO my_tbl (\"email\", \"age\", \"username\") VALUES (?, 20, ?)",
            },
            // Empty column
            TC {
                cols: &cols3,
                want_mysql: "",
                want_postgresql: "",
                want_sqlite: "",
            },
        ];

        for tc in test_cases {
            let sb_mysql = StmtBuilder::new(String::from(TABLE), Type::MySQL);
            assert_eq!(sb_mysql.build_insert_stmt(tc.cols), tc.want_mysql);

            let sb_postgresql = StmtBuilder::new(String::from(TABLE), Type::PostgreSQL);
            assert_eq!(sb_postgresql.build_insert_stmt(tc.cols), tc.want_postgresql);

            let sb_sqlite = StmtBuilder::new(String::from(TABLE), Type::SQLite);
            assert_eq!(sb_sqlite.build_insert_stmt(tc.cols), tc.want_sqlite);
        }
    }

    #[test]
    fn test_build_query_stmt() {
        struct TC<'a> {
            cols: &'a [String],
            conds: &'a [KV<'a>],
            want_mysql: &'a str,
            want_postgresql: &'a str,
            want_sqlite: &'a str,
        }

        let cols1 = vec![String::from("username")];
        let conds1 = vec![KV {
            key: "id",
            val: PLACEHOLDER,
        }];

        let cols2 = vec![String::from("username"), String::from("nickname")];
        let conds2 = vec![
            KV {
                key: "name",
                val: PLACEHOLDER,
            },
            KV {
                key: "age",
                val: "20",
            },
            KV {
                key: "gender",
                val: PLACEHOLDER,
            },
        ];

        let cols3 = vec![String::from("*")];
        let conds3 = vec![
            KV {
                key: "name",
                val: PLACEHOLDER,
            },
            KV {
                key: "age",
                val: "20",
            },
            KV {
                key: "gender",
                val: PLACEHOLDER,
            },
        ];

        let cols4: Vec<String> = Vec::new();
        let conds4: Vec<KV<'_>> = Vec::new();

        let test_cases = vec![
            // Single column and condition
            TC {
                cols: &cols1,
                conds: &conds1,
                want_mysql: "SELECT `username` FROM my_tbl WHERE id = ?",
                want_postgresql: "SELECT \"username\" FROM my_tbl WHERE id = $1",
                want_sqlite: "SELECT \"username\" FROM my_tbl WHERE id = ?",
            },
            // Multiple columns and conditions
            TC {
                cols: &cols2,
                conds: &conds2,
                want_mysql: "SELECT `username`, `nickname` FROM my_tbl WHERE name = ? AND age = 20 AND gender = ?",
                want_postgresql: "SELECT \"username\", \"nickname\" FROM my_tbl WHERE name = $1 AND age = 20 AND gender = $2",
                want_sqlite: "SELECT \"username\", \"nickname\" FROM my_tbl WHERE name = ? AND age = 20 AND gender = ?",
            },
            // Select all columns
            TC {
                cols: &cols3,
                conds: &conds3,
                want_mysql: "SELECT * FROM my_tbl WHERE name = ? AND age = 20 AND gender = ?",
                want_postgresql: "SELECT * FROM my_tbl WHERE name = $1 AND age = 20 AND gender = $2",
                want_sqlite: "SELECT * FROM my_tbl WHERE name = ? AND age = 20 AND gender = ?",
            },
            // Empty columns and conditions
            TC {
                cols: &cols4,
                conds: &conds4,
                want_mysql: "SELECT * FROM my_tbl",
                want_postgresql: "SELECT * FROM my_tbl",
                want_sqlite: "SELECT * FROM my_tbl",
            },
        ];

        for tc in test_cases {
            let sb_mysql = StmtBuilder::new(String::from(TABLE), Type::MySQL);
            assert_eq!(sb_mysql.build_query_stmt(tc.cols, tc.conds), tc.want_mysql);

            let sb_postgresql = StmtBuilder::new(String::from(TABLE), Type::PostgreSQL);
            assert_eq!(
                sb_postgresql.build_query_stmt(tc.cols, tc.conds),
                tc.want_postgresql
            );

            let sb_sqlite = StmtBuilder::new(String::from(TABLE), Type::SQLite);
            assert_eq!(
                sb_sqlite.build_query_stmt(tc.cols, tc.conds),
                tc.want_sqlite
            );
        }
    }

    #[test]
    fn test_build_update_stmt() {
        struct TC<'a> {
            cols: &'a [KV<'a>],
            conds: &'a [KV<'a>],
            want_mysql: &'a str,
            want_postgresql: &'a str,
            want_sqlite: &'a str,
        }

        let cols1 = vec![
            KV {
                key: "age",
                val: "20",
            },
            KV {
                key: "username",
                val: PLACEHOLDER,
            },
            KV {
                key: "nickname",
                val: PLACEHOLDER,
            },
        ];
        let conds1 = vec![
            KV {
                key: "id",
                val: PLACEHOLDER,
            },
            KV {
                key: "status",
                val: "'active'",
            },
        ];

        let cols2: Vec<KV<'_>> = Vec::new();
        let conds2: Vec<KV<'_>> = Vec::new();

        let test_cases = vec![
            // Multiple columns and conditions
            TC {
                cols: &cols1,
                conds: &conds1,
                want_mysql: "UPDATE my_tbl SET `age` = 20, `username` = ?, `nickname` = ? WHERE id = ? AND status = 'active'",
                want_postgresql: "UPDATE my_tbl SET \"age\" = 20, \"username\" = $1, \"nickname\" = $2 WHERE id = $3 AND status = 'active'",
                want_sqlite: "UPDATE my_tbl SET \"age\" = 20, \"username\" = ?, \"nickname\" = ? WHERE id = ? AND status = 'active'",
            },
            // Empty columns
            TC {
                cols: &cols2,
                conds: &conds1,
                want_mysql: "",
                want_postgresql: "",
                want_sqlite: "",
            },
            // Empty conditions
            TC {
                cols: &cols1,
                conds: &conds2,
                want_mysql: "UPDATE my_tbl SET `age` = 20, `username` = ?, `nickname` = ?",
                want_postgresql: "UPDATE my_tbl SET \"age\" = 20, \"username\" = $1, \"nickname\" = $2",
                want_sqlite: "UPDATE my_tbl SET \"age\" = 20, \"username\" = ?, \"nickname\" = ?",
            },
        ];

        for tc in test_cases {
            let sb_mysql = StmtBuilder::new(String::from(TABLE), Type::MySQL);
            assert_eq!(sb_mysql.build_update_stmt(tc.cols, tc.conds), tc.want_mysql);

            let sb_postgresql = StmtBuilder::new(String::from(TABLE), Type::PostgreSQL);
            assert_eq!(
                sb_postgresql.build_update_stmt(tc.cols, tc.conds),
                tc.want_postgresql
            );

            let sb_sqlite = StmtBuilder::new(String::from(TABLE), Type::SQLite);
            assert_eq!(
                sb_sqlite.build_update_stmt(tc.cols, tc.conds),
                tc.want_sqlite
            );
        }
    }

    #[test]
    fn test_build_delete_stmt() {
        struct TC<'a> {
            conds: &'a [KV<'a>],
            want_mysql: &'a str,
            want_postgresql: &'a str,
            want_sqlite: &'a str,
        }

        let conds1 = vec![KV {
            key: "username",
            val: PLACEHOLDER,
        }];
        let conds2 = vec![
            KV {
                key: "username",
                val: PLACEHOLDER,
            },
            KV {
                key: "nickname",
                val: "'foo'",
            },
        ];
        let conds3: Vec<KV<'_>> = Vec::new();

        let test_cases = vec![
            // Single column
            TC {
                conds: &conds1,
                want_mysql: "DELETE FROM my_tbl WHERE username = ?",
                want_postgresql: "DELETE FROM my_tbl WHERE username = $1",
                want_sqlite: "DELETE FROM my_tbl WHERE username = ?",
            },
            // Multiple columns
            TC {
                conds: &conds2,
                want_mysql: "DELETE FROM my_tbl WHERE username = ? AND nickname = 'foo'",
                want_postgresql: "DELETE FROM my_tbl WHERE username = $1 AND nickname = 'foo'",
                want_sqlite: "DELETE FROM my_tbl WHERE username = ? AND nickname = 'foo'",
            },
            // Empty conditions
            TC {
                conds: &conds3,
                want_mysql: "DELETE FROM my_tbl",
                want_postgresql: "DELETE FROM my_tbl",
                want_sqlite: "DELETE FROM my_tbl",
            },
        ];

        for tc in test_cases {
            let sb_mysql = StmtBuilder::new(String::from(TABLE), Type::MySQL);
            assert_eq!(sb_mysql.build_delete_stmt(tc.conds), tc.want_mysql);

            let sb_postgresql = StmtBuilder::new(String::from(TABLE), Type::PostgreSQL);
            assert_eq!(
                sb_postgresql.build_delete_stmt(tc.conds),
                tc.want_postgresql
            );

            let sb_sqlite = StmtBuilder::new(String::from(TABLE), Type::SQLite);
            assert_eq!(sb_sqlite.build_delete_stmt(tc.conds), tc.want_sqlite);
        }
    }
}
