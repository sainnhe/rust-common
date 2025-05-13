//! Utilities for [`sqlx`].

/// Key-value pair that can be used in building SQL statements.
#[derive(PartialEq, Eq, Clone, Copy, Hash, Debug)]
pub struct KV<'a> {
    pub key: &'a str,
    pub val: &'a str,
}

/// Builds SQL statements that can be used in [`sqlx`].
///
/// Since the builder will use string replacement to build SQL statements, please make sure the
/// values used here, for example the table name and the column names, are safe and won't lead to
/// SQL injection.
///
/// You can use placeholders in the passed values. [`sqlx`] will bind them to the corresponding
/// arguments, and this process is safe and won't lead to SQL injection.
///
/// The placeholders used in different database types are as follows:
///
/// * MySQL: ?
/// * PostgreSQL: $N, where N is the 1-based positional argument index.
/// * SQLite: ?
pub struct StmtBuilder {
    tbl: String,
}

impl StmtBuilder {
    /// Creates a new StmtBuilder, where tbl is the table name.
    ///
    /// NOTE: Since the table name will be embedded directly into the final SQL statement, make sure that
    /// the table name is safe and will not lead to SQL injection.
    pub fn new(tbl: String) -> StmtBuilder {
        StmtBuilder { tbl }
    }

    /// Gets the table name.
    pub fn get_tbl(&self) -> &String {
        &self.tbl
    }

    fn build_conds(&self, conds: &[KV]) -> String {
        if conds.is_empty() {
            String::new()
        } else {
            format!(
                " WHERE {}",
                conds
                    .iter()
                    .map(|kv| format!("{} = {}", kv.key, kv.val))
                    .collect::<Vec<String>>()
                    .join(" AND ")
            )
        }
    }

    /// Builds a SQL statement that performs insert operation.
    ///
    /// # Arguments
    ///
    /// * `cols` - The column names and values to be inserted.
    ///
    /// # Returns
    ///
    /// * The SQL statement. An empty string will be returned if the `cols` argument is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use sainnhe_common::db::sqlx::{KV, StmtBuilder};
    ///
    /// let sb = StmtBuilder::new(String::from("my_tbl"));
    /// let cols = vec![
    ///     KV {
    ///         key: "username",
    ///         val: "?",
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
    ///     "INSERT INTO my_tbl (username, nickname, create_at) VALUES (?, 'foo', NOW())";
    ///
    /// assert_eq!(stmt, expected_stmt);
    /// ```
    pub fn build_insert_stmt(&self, cols: &[KV]) -> String {
        if cols.is_empty() {
            return String::new();
        }
        let (keys, vals): (Vec<&str>, Vec<&str>) = cols.iter().map(|kv| (kv.key, kv.val)).unzip();
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
    /// * `cols` - The selected columns. If it's empty, ["*"] will be used.
    /// * `conds` - The equal conditions.
    ///
    /// # Returns
    ///
    /// * The SQL statement.
    ///
    /// # Examples
    ///
    /// ```
    /// use sainnhe_common::db::sqlx::{KV, StmtBuilder};
    ///
    /// let sb = StmtBuilder::new(String::from("my_tbl"));
    /// let cols = vec![String::from("age"), String::from("gender")];
    /// let conds = vec![
    ///     KV {
    ///         key: "username",
    ///         val: "$1",
    ///     },
    ///     KV {
    ///         key: "nickname",
    ///         val: "'foo'",
    ///     },
    /// ];
    ///
    /// let stmt = sb.build_query_stmt(&cols, &conds);
    /// let expected_stmt = "SELECT age, gender FROM my_tbl WHERE username = $1 AND nickname = 'foo'";
    ///
    /// assert_eq!(stmt, expected_stmt);
    /// ```
    pub fn build_query_stmt(&self, cols: &[String], conds: &[KV]) -> String {
        let cols_str = if cols.is_empty() {
            String::from("*")
        } else {
            cols.join(", ")
        };
        format!(
            "SELECT {} FROM {}{}",
            cols_str,
            self.tbl,
            self.build_conds(conds)
        )
    }

    /// Builds a SQL statement that performs update operation.
    ///
    /// # Arguments
    ///
    /// * `cols` - The column names and values to be inserted.
    /// * `conds` - The equal conditions.
    ///
    /// # Returns
    ///
    /// * The SQL statement.
    ///
    /// # Examples
    ///
    /// ```
    /// use sainnhe_common::db::sqlx::{KV, StmtBuilder};
    ///
    /// let sb = StmtBuilder::new(String::from("my_tbl"));
    /// let cols = vec![
    ///     KV {
    ///         key: "username",
    ///         val: "?",
    ///     },
    ///     KV {
    ///         key: "nickname",
    ///         val: "'foo'",
    ///     },
    ///     KV {
    ///         key: "update_at",
    ///         val: "NOW()",
    ///     },
    /// ];
    /// let conds = vec![
    ///     KV {
    ///         key: "age",
    ///         val: "?",
    ///     },
    ///     KV {
    ///         key: "gender",
    ///         val: "'male'",
    ///     },
    /// ];
    ///
    /// let stmt = sb.build_update_stmt(&cols, &conds);
    /// let expected_stmt = "UPDATE my_tbl SET username = ?, nickname = 'foo', update_at = NOW() WHERE age = ? AND gender = 'male'";
    ///
    /// assert_eq!(stmt, expected_stmt);
    /// ```
    pub fn build_update_stmt(&self, cols: &[KV], conds: &[KV]) -> String {
        if cols.is_empty() {
            return String::new();
        }
        format!(
            "UPDATE {} SET {}{}",
            self.tbl,
            cols.iter()
                .map(|kv| format!("{} = {}", kv.key, kv.val))
                .collect::<Vec<String>>()
                .join(", "),
            self.build_conds(conds)
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
    /// use sainnhe_common::db::sqlx::{KV, StmtBuilder};
    ///
    /// let sb = StmtBuilder::new(String::from("my_tbl"));
    /// let conds = vec![
    ///     KV {
    ///         key: "username",
    ///         val: "$1",
    ///     },
    ///     KV {
    ///         key: "nickname",
    ///         val: "'foo'",
    ///     },
    /// ];
    ///
    /// let stmt = sb.build_delete_stmt(&conds);
    /// let expected_stmt = "DELETE FROM my_tbl WHERE username = $1 AND nickname = 'foo'";
    ///
    /// assert_eq!(stmt, expected_stmt);
    /// ```
    pub fn build_delete_stmt(&self, conds: &[KV]) -> String {
        format!("DELETE FROM {}{}", self.tbl, self.build_conds(conds))
    }
}

#[cfg(test)]
mod tests {
    use super::{KV, StmtBuilder};

    static TABLE: &str = "my_tbl";

    #[test]
    fn test_build_insert_stmt() {
        struct TC<'a> {
            cols: &'a [KV<'a>],
            want: &'a str,
        }

        let cols1 = vec![KV {
            key: "username",
            val: "?",
        }];
        let cols2 = vec![
            KV {
                key: "username",
                val: "?",
            },
            KV {
                key: "nickname",
                val: "'foo'",
            },
            KV {
                key: "create_at",
                val: "NOW()",
            },
        ];
        let cols3: Vec<KV<'_>> = Vec::new();

        let test_cases = vec![
            // Single column
            TC {
                cols: &cols1,
                want: "INSERT INTO my_tbl (username) VALUES (?)",
            },
            // Multiple columns
            TC {
                cols: &cols2,
                want: "INSERT INTO my_tbl (username, nickname, create_at) VALUES (?, 'foo', NOW())",
            },
            // Empty column
            TC {
                cols: &cols3,
                want: "",
            },
        ];

        for tc in test_cases {
            let b = StmtBuilder::new(String::from(TABLE));
            assert_eq!(b.build_insert_stmt(tc.cols), tc.want);
        }
    }

    #[test]
    fn test_build_query_stmt() {
        struct TC<'a> {
            cols: &'a [String],
            conds: &'a [KV<'a>],
            want: &'a str,
        }

        let cols1 = vec![String::from("age")];
        let conds1 = vec![KV {
            key: "username",
            val: "?",
        }];

        let cols2 = vec![String::from("age"), String::from("gender")];
        let conds2 = vec![
            KV {
                key: "username",
                val: "?",
            },
            KV {
                key: "nickname",
                val: "'foo'",
            },
        ];

        let cols3: Vec<String> = Vec::new();
        let conds3: Vec<KV<'_>> = Vec::new();

        let test_cases = vec![
            // Single column
            TC {
                cols: &cols1,
                conds: &conds1,
                want: "SELECT age FROM my_tbl WHERE username = ?",
            },
            // Multiple columns
            TC {
                cols: &cols2,
                conds: &conds2,
                want: "SELECT age, gender FROM my_tbl WHERE username = ? AND nickname = 'foo'",
            },
            // Empty columns and conditions
            TC {
                cols: &cols3,
                conds: &conds3,
                want: "SELECT * FROM my_tbl",
            },
        ];

        for tc in test_cases {
            let b = StmtBuilder::new(String::from(TABLE));
            assert_eq!(b.build_query_stmt(tc.cols, tc.conds), tc.want);
        }
    }

    #[test]
    fn test_build_update_stmt() {
        struct TC<'a> {
            cols: &'a [KV<'a>],
            conds: &'a [KV<'a>],
            want: &'a str,
        }

        let cols1 = vec![KV {
            key: "age",
            val: "?",
        }];
        let conds1 = vec![KV {
            key: "username",
            val: "?",
        }];

        let cols2 = vec![
            KV {
                key: "age",
                val: "?",
            },
            KV {
                key: "gender",
                val: "'male'",
            },
        ];
        let conds2 = vec![
            KV {
                key: "username",
                val: "?",
            },
            KV {
                key: "nickname",
                val: "'foo'",
            },
        ];

        let cols3: Vec<KV<'_>> = Vec::new();
        let conds3: Vec<KV<'_>> = Vec::new();

        let test_cases = vec![
            // Single column
            TC {
                cols: &cols1,
                conds: &conds1,
                want: "UPDATE my_tbl SET age = ? WHERE username = ?",
            },
            // Multiple columns
            TC {
                cols: &cols2,
                conds: &conds2,
                want: "UPDATE my_tbl SET age = ?, gender = 'male' WHERE username = ? AND nickname = 'foo'",
            },
            // Empty columns
            TC {
                cols: &cols3,
                conds: &conds1,
                want: "",
            },
            // Empty conditions
            TC {
                cols: &cols1,
                conds: &conds3,
                want: "UPDATE my_tbl SET age = ?",
            },
        ];

        for tc in test_cases {
            let b = StmtBuilder::new(String::from(TABLE));
            assert_eq!(b.build_update_stmt(tc.cols, tc.conds), tc.want);
        }
    }

    #[test]
    fn test_build_delete_stmt() {
        struct TC<'a> {
            conds: &'a [KV<'a>],
            want: &'a str,
        }

        let conds1 = vec![KV {
            key: "username",
            val: "?",
        }];
        let conds2 = vec![
            KV {
                key: "username",
                val: "?",
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
                want: "DELETE FROM my_tbl WHERE username = ?",
            },
            // Multiple columns
            TC {
                conds: &conds2,
                want: "DELETE FROM my_tbl WHERE username = ? AND nickname = 'foo'",
            },
            // Empty conditions
            TC {
                conds: &conds3,
                want: "DELETE FROM my_tbl",
            },
        ];

        for tc in test_cases {
            let b = StmtBuilder::new(String::from(TABLE));
            assert_eq!(b.build_delete_stmt(tc.conds), tc.want);
        }
    }
}
