use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
mod data_descriptor;
mod executor;
mod validators;
mod raw_inner_value;
use crate::executor::{Execute, SuccessStatus};
use crate::data_descriptor::DataBase;
use std::time::Instant;
fn main() {
    let rows_to_insert = 1000000;
      let mut sql = "CREATE TABLE IF NOT EXISTS users (
          id bigint(20) PRIMARY KEY AUTO_INCREMENT,
          name VARCHAR(255) NOT NULL,
          password VARCHAR(255) NOT NULL
        );
      ".to_string();

    sql.push_str(" INSERT INTO `users` (`id`, `name`, `password`) VALUES
    (1, 'admin', '$2b$10$I6gmAyHE0r3TGZEnDP0O5efjsuBXVT36M.HLyZDm4cuJFyJcqXdG.'),
    (2, 'armando', '$2b$10$qQEy7J/lN3XXP.Gp1wv3zO2SjpOp3NmsACA2TroekCYVUa55K6T8i'),");

    for n in 3..rows_to_insert {
        sql.push_str(&format!("({}, 'armando{}', '$2b$10$qQEy7J/lN3XXP.Gp1wv3zO2SjpOp3NmsACA2TroekCYVUa55K6T8i'){}",n,n, 
        if (n+1) == rows_to_insert {
          ";"
        } else {
          ","
        }));
    }
    sql.push_str(" SELECT * from users;");
    //println!("{}", sql);

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let mut db = DataBase::new();

    let ast = Parser::parse_sql(&dialect, &sql).unwrap();

    for expr in ast {
      let start_time = Instant::now();
      let res = expr.execute(&mut db);
      let elapsed_time = start_time.elapsed();
      println!("query time: {:?}", elapsed_time);
      match res {
        Ok(SuccessStatus::DataFetched(_data)) => {
          //println!("expr result: {}", json_data);
        },
        _=>{ 
          //println!("expr result: {:#?}", res); 
        }
      }
    }

    //println!("AST: {:#?}", db);
}
