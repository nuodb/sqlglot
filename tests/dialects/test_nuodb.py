from tests.dialects.test_dialect import Validator


class TestNuoDB(Validator):
    dialect = "nuodb"

    def test_ddl(self):
        self.validate_all("UPDATE teams SET wins = wins + 1 WHERE teamid IN ('COB', 'BOS', 'AND');")
        self.validate_all("DELETE FROM teams WHERE teamid IN ('COB', 'BOS', 'AND');")
        self.validate_all(
            "INSERT INTO hockey.hockey (number, name, position, team) VALUES (99, 'TOM JONES', 'Goalie', 'Bruins');"
        )
        self.validate_all(
            "CREATE TABLE employees (id INT, first_name VARCHAR(50), last_name VARCHAR(50), salary DECIMAL(10, 2))",
            write={
                "nuodb": "CREATE TABLE employees (id INT, first_name VARCHAR(50), last_name VARCHAR(50), salary DECIMAL(10, 2))",
                "mysql": "CREATE TABLE employees (id INT, first_name VARCHAR(50), last_name VARCHAR(50), salary DECIMAL(10, 2))",
                "postgres": "CREATE TABLE employees (id INT, first_name VARCHAR(50), last_name VARCHAR(50), salary DECIMAL(10, 2))",
                "tsql": "CREATE TABLE employees (id INTEGER, first_name VARCHAR(50), last_name VARCHAR(50), salary NUMERIC(10, 2))",
                "oracle": "CREATE TABLE employees (id NUMBER, first_name VARCHAR2(50), last_name VARCHAR2(50), salary NUMBER(10, 2))",
            },
        )

        self.validate_all(
            "ALTER TABLE employees ADD COLUMN hire_date DATE",
            write={"nuodb": "ALTER TABLE employees ADD COLUMN hire_date DATE"},
        )

        # ? CONSIDER MOVING TO MYSQL TESTS
        self.validate_all(
            "CREATE TABLE `datatypes1` (`c6` mediumint(9) NOT NULL DEFAULT '0')",
            read="mysql",
            write={"nuodb": "CREATE TABLE `datatypes1` (`c6` INTEGER(9) NOT NULL DEFAULT '0')"},
        )
