from __future__ import annotations

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import Dialect, no_comment_column_constraint_sql
from sqlglot.errors import UnsupportedError
from sqlglot.tokens import Tokenizer, TokenType

global schema_name
schema_name = None


def _parse_introducer(self: generator.Generator, expression: exp.Expression) -> exp.Expression:
    expression.args["this"] = None
    return expression


def _parse_fulltext_key(self: generator.Generator, expression: exp.Expression) -> None:
    self.unsupported("FULLTEXT KEY is not supported in NuoDB")


def _parse_spatial_key(self: generator.Generator, expression: exp.Expression) -> None:
    # NuoDB doesn't support Spatial key, so just returning it as Null
    raise UnsupportedError("SPATIAL KEY is not supported in NuoDB")


# prefix index
def _parse_key_constraint(
    self: generator.Generator, expression: exp.Expression, k: exp.Expression
) -> None:
    fun_exp = k.find_all(exp.Func)
    datatype = "INT"
    for f in fun_exp:
        col_name_for_ind = f.args["this"]
        numeric = f.args["expressions"][0]
        new_column_name = f"prefix_index_{col_name_for_ind}"
        if k.args["colname"].this == f:
            col_exp = expression.find_all(exp.ColumnDef)
            for column in col_exp:
                col = column.args["this"]
                if isinstance(col, exp.Identifier):
                    col = col.this
                if col == col_name_for_ind:
                    datatype = column.args["kind"]
        k.pop()
        new_col = exp.ColumnDef(this=new_column_name, quoted=True, kind=datatype)
        expr = f"LEFT({col_name_for_ind}, {numeric})"
        new_col.append(
            "constraints",
            exp.ColumnConstraint(
                kind=exp.GeneratedAsIdentityColumnConstraint(
                    this=True, expression=expr, stored=exp.Identifier(this="PERSISTED")
                )
            ),
        )
        schema = expression.this
        schema.append("expressions", new_col)
        k.args["colname"] = f"({new_column_name})"
        schema.append("expressions", k)


def no_properties_sql(self: generator.Generator, expression: exp.Properties) -> str:
    if isinstance(expression, exp.PartitionedByProperty):
        return ""
    self.unsupported("Properties unsupported")
    return ""

def _parse_fk(self: generator.Generator, expression: exp.Expression) -> exp.Expression | None:
    if expression.parent:
        if isinstance(expression.parent.parent, exp.Create):
            global schema_name
            index_foreign_key_sql = ""
            alter_table = ""
            key_name = ""
            foreign_key_expression = expression.find_all(exp.ForeignKey)

            Key_constraint_index = expression.parent.parent.find_all(
                exp.KeyColumnConstraintForIndex
            )
            key_col_map = {}

            ref_key_index = expression.find_all(exp.Reference)
            if ref_key_index:
                for r in ref_key_index:
                    options_list = r.args.get("options", [])
                    if options_list and len(options_list) > 0:
                        first_option = options_list[0]
                        r.args["options"] = [first_option]

            for k in Key_constraint_index:
                idx_name = str(k.args.get("keyname"))
                key_name = k.args["colname"]
                key_name = key_name.this  # type: ignore
                key_col_map[str(idx_name)] = str(key_name)
            if foreign_key_expression:
                for fk in foreign_key_expression:
                    tbl_name = expression.parent.args["this"]
                    # multiple columnss
                    for f in fk.args["expressions"]:
                        column_name = f
                        index_name = f"{tbl_name}_{column_name}"
                        index_name = index_name.replace('"', "")
                        # constraint_name = expression.args["this"]
                        constraint_name = f"{column_name}_fk"
                        if len(key_col_map) != 0:
                            for idx_name, key_name in key_col_map.items():
                                if str(column_name) == str(key_name) or str(idx_name) == str(
                                    constraint_name
                                ):
                                    index_foreign_key_sql = ""
                                    break
                                else:
                                    index_foreign_key_sql = (
                                        f"CREATE INDEX {index_name} ON {tbl_name} ({column_name})"
                                    )
                        else:
                            index_foreign_key_sql = (
                                f"CREATE INDEX {index_name} ON {tbl_name} ({column_name})"
                            )
                        if schema_name:
                            alter_table = f"ALTER TABLE {schema_name}.{tbl_name} ADD CONSTRAINT {constraint_name} {expression}"
                        else:
                            alter_table = f"ALTER TABLE {tbl_name} ADD CONSTRAINT {constraint_name} {expression}"
                        expression.parent.parent.add_foreign_key_index(index_foreign_key_sql)
                    expression.parent.parent.add_foreign_key_constraint(alter_table)

    if isinstance(expression.parent, exp.AlterTable):
        foreign_key_expression = expression.find_all(exp.ForeignKey)
        index_foreign_key_sql = ""
        if foreign_key_expression:
            for fk in foreign_key_expression:
                tbl_name = expression.parent.args["this"]
                column_name = fk.args["expressions"][0]
                index_name = f"{tbl_name}_{column_name}"
                index_name = index_name.replace('"', "")
                index_foreign_key_sql = f"CREATE INDEX {index_name} ON {tbl_name} ({column_name})"
                expression.parent.set("foreign_key_index", index_foreign_key_sql)
    if generator.exclude_fk_constraint:
        return None

    return expression

def _parse_foreign_key_index(
    self: generator.Generator, expression: exp.Expression
) -> exp.Expression | None:
    if expression.parent:
        if isinstance(expression.parent.parent, exp.Create):
            global schema_name
            index_foreign_key_sql = ""
            alter_table = ""
            key_name = ""
            foreign_key_expression = expression.find_all(exp.ForeignKey)

            Key_constraint_index = expression.parent.parent.find_all(
                exp.KeyColumnConstraintForIndex
            )
            key_col_map = {}

            ref_key_index = expression.find_all(exp.Reference)
            if ref_key_index:
                for r in ref_key_index:
                    options_list = r.args.get("options", [])
                    if options_list and len(options_list) > 0:
                        first_option = options_list[0]
                        r.args["options"] = [first_option]

            for k in Key_constraint_index:
                idx_name = str(k.args.get("keyname"))
                key_name = k.args["colname"]
                key_name = key_name.this  # type: ignore
                key_col_map[str(idx_name)] = str(key_name)
            if foreign_key_expression:
                for fk in foreign_key_expression:
                    tbl_name = expression.parent.args["this"]
                    # multiple columnss
                    for f in fk.args["expressions"]:
                        column_name = f.this
                        index_name = f"{tbl_name}_{column_name}"
                        index_name = index_name.replace('"', "")
                        constraint_name = expression.args["this"]
                        if len(key_col_map) != 0:
                            for idx_name, key_name in key_col_map.items():
                                if str(column_name) == str(key_name) or str(idx_name) == str(
                                    constraint_name
                                ):
                                    index_foreign_key_sql = ""
                                    break
                                else:
                                    index_foreign_key_sql = (
                                        f"CREATE INDEX {index_name} ON {tbl_name} ({column_name})"
                                    )
                        else:
                            index_foreign_key_sql = (
                                f"CREATE INDEX {index_name} ON {tbl_name} ({column_name})"
                            )
                        if schema_name:
                            alter_table = f"ALTER TABLE {schema_name}.{tbl_name} ADD {expression}"
                        else:
                            alter_table = f"ALTER TABLE {tbl_name} ADD {expression}"
                        expression.parent.parent.add_foreign_key_index(index_foreign_key_sql)
                    expression.parent.parent.add_foreign_key_constraint(alter_table)

    if isinstance(expression.parent, exp.AlterTable):
        foreign_key_expression = expression.find_all(exp.ForeignKey)
        index_foreign_key_sql = ""
        if foreign_key_expression:
            for fk in foreign_key_expression:
                tbl_name = expression.parent.args["this"]
                column_name = fk.args["expressions"][0]
                index_name = f"{tbl_name}_{column_name}"
                index_name = index_name.replace('"', "")
                index_foreign_key_sql = f"CREATE INDEX {index_name} ON {tbl_name} ({column_name})"
                expression.parent.set("foreign_key_index", index_foreign_key_sql)
    if generator.exclude_fk_constraint:
        return None

    return expression


def _auto_increment_to_generated_by_default(expression: exp.Expression) -> exp.Expression:
    auto = expression.find(exp.AutoIncrementColumnConstraint)
    if auto:
        expression = expression.copy()
        constraints = expression.args["constraints"]
        expression.args["constraints"].remove(auto.parent)
        generated = exp.ColumnConstraint(
            kind=exp.GeneratedAsIdentityColumnConstraint(this=False, stored=False)
        )
        if generated not in constraints:
            constraints.insert(0, generated)

    generatedColumn = expression.find(exp.GeneratedAsIdentityColumnConstraint)
    if generatedColumn:
        if generatedColumn.args["this"] is False:
            if generatedColumn.args["start"] is not None or False:
                generatedColumn.args["start"] = False

    return expression


def _parse_partition_hash(self: generator.Generator, expression: exp.Expression):
    schema = expression.this
    partition_exp = expression.find_all(exp.PartitionedByProperty)
    for p in partition_exp:
        function_exp = p.find_all(exp.Func)
        col_name = p.args["this"]
        exprssion_col_name = p.args["this"]
        if function_exp:
            for fun in function_exp:
                col_name = fun.args["this"]
                col_name = col_name.this
        partition_col_name = f"p_{col_name}"
        partition_col = exp.ColumnDef(this=partition_col_name, kind="INT")
        partition_col.append(
            "constraints",
            exp.ColumnConstraint(
                kind=exp.GeneratedAsIdentityColumnConstraint(
                    this=True, expression=f"{exprssion_col_name}%4", stored=exp.Identifier(this="PERSISTED")
                )
            ),
        )
        schema.append("expressions", partition_col)
        p.args["this"] = f"({partition_col_name})"
        count_partitions = p.args["count_partitions"]
        sub_part_list = []
        for i in range(0, int(count_partitions)):
            part_name = f"p{i}"
            sub_exp = f"PARTITION {part_name} VALUES IN ({i}) STORE IN UNPARTITIONED"
            sub_part_list.append(sub_exp)
        p.args["main_partition"] = partition_col_name
        p.args["subpart_exp"] = sub_part_list
        p.args["type"] = "LIST"
        p.args["subpartition"] = True


def _parse_partition_key(self: generator.Generator, expression: exp.Expression):

    column_constraint = expression.find_all(exp.ColumnDef)
    partition_key_column_name = None
    partition_exp = expression.find_all(exp.PartitionedByProperty)
    unique_key_exp = expression.find_all(exp.UniqueColumnConstraint)

    for p in partition_exp:
        part_col = p.args["main_partition"]
        count_partitions = p.args["count_partitions"]
        if part_col and part_col != "":
            partition_key_column_name = part_col
        else:
            if column_constraint:
                for col in column_constraint:
                    constraints = col.args["constraints"]
                    for const in constraints:
                        if isinstance(const.kind, exp.PrimaryKeyColumnConstraint):
                            partition_key_column_name = col.args["this"]
                        elif unique_key_exp:
                            for uniq in unique_key_exp:
                                partition_key_column_name = col.args["this"]

        sub_part_list = []
        for i in range(0, int(count_partitions)):
            part_name = f"p{i}"
            sub_exp = f"PARTITION {part_name} VALUES IN ({i}) STORE IN UNPARTITIONED"
            sub_part_list.append(sub_exp)
        p.args["type"] = "LIST"
        p.args["subpartition"] = True
        p.args["subpart_exp"] = sub_part_list

        p.args["main_partition"] = f"{partition_key_column_name}"


def _parse_partition_range(self: generator.Generator, expression: exp.Expression):
    schema = expression.this
    processed_functions = set()
    partition_exp = expression.find_all(exp.PartitionedByProperty)
    for p in partition_exp:
        function_exp = p.find_all(exp.Func)
        if function_exp:
            for fun in function_exp:
                col_name = fun.args["this"]
                partition_col_name = f"partition_col"
                if fun in processed_functions:
                    continue
                partition_column = exp.ColumnDef(this=partition_col_name, kind="INT")
                partition_column.append(
                    "constraints",
                    exp.ColumnConstraint(
                        kind=exp.GeneratedAsIdentityColumnConstraint(
                            this=True, expression=fun, stored=exp.Identifier(this="PERSISTED")
                        )
                    ),
                )
                schema.append("expressions", partition_column)
                p.args["this"] = f"({partition_col_name})"
                p.args["main_partition"] = partition_col_name
                processed_functions.add(fun)


def replace_db_to_schema(self: generator.Generator, expression: exp.Create) -> str:
    if isinstance(expression, (exp.Create)) and expression.args["kind"] == "DATABASE":
        expression.args["kind"] = "SCHEMA"
        global schema_name
        schema_name = expression.args["this"]

    isinstance(expression.this, exp.Schema)
    is_partitionable = expression.args.get("kind") in ("TABLE", "VIEW")

    if (isinstance(expression, exp.Create)) and is_partitionable:
        partition_exp = expression.find_all(exp.PartitionedByProperty)
        for p in partition_exp:
            if p.args["type"] == "RANGE":
                _parse_partition_range(self, expression)
            if p.args["type"] == "LIST":
                _parse_partition_range(self, expression)
            if p.args["type"] == "HASH":
                _parse_partition_hash(self, expression)
            if p.args["type"] == "KEY":
                _parse_partition_key(self, expression)
            if p.args["type"] == "RANGE COLUMNS":
                if self.sql(expression, "this"):
                    self.unsupported("RANGE COLUMNS are not supported")
                    return ""

    if isinstance(expression, exp.Create):
        key_const_exp = expression.find_all(exp.KeyColumnConstraintForIndex)
        if key_const_exp:
            for k in key_const_exp:
                _parse_key_constraint(self, expression, k)

    return self.create_sql(expression)


def _parse_unique(self: generator.Generator, expression: exp.Expression) -> str:
    unique = expression.find_all(exp.UniqueColumnConstraint)
    if unique:
        this = expression.args["this"]
    return f"UNIQUE KEY {this}"


def _remove_collate(expression: exp.Expression) -> exp.Expression:
    column_constraint = expression.find(exp.ColumnConstraint)
    if column_constraint:
        expression = expression.copy()
        collateProp = column_constraint.find(exp.CollateColumnConstraint)
        charsetProp = column_constraint.find(exp.CharacterSetColumnConstraint)
        onUpdateColumnConstraint = column_constraint.find(exp.OnUpdateColumnConstraint)
        defaultColumnConstraint = column_constraint.find(exp.DefaultColumnConstraint)

        if onUpdateColumnConstraint:
            expression.args["kind"].replace(None)

        if collateProp or charsetProp:
            expression.args["kind"].replace(None)
        if defaultColumnConstraint:
            currentTime = defaultColumnConstraint.find(exp.CurrentTimestamp)
            if currentTime:
                expression.set(
                    "kind", exp.DefaultColumnConstraint(this=exp.CurrentTimestamp(this=None))
                )

    # logic to change storage in computed column
    compColumn = expression.find_all(exp.GeneratedAsIdentityColumnConstraint)
    if compColumn:
        for col in compColumn:
            introducers = col.find_all(exp.Introducer)
            if introducers:
                for i in introducers:
                    i.args["this"] = None
            if col.args["this"]:
                if col.args["stored"].name == "VIRTUAL" or col.args["stored"].name == "STORED":
                    col.args["stored"] = exp.Identifier(this="PERSISTED")

    return expression


class NuoDB(Dialect):
    # * Refer to http://nuocrucible/browse/NuoDB/Omega/Parser/SQL.l?r=5926eff6ff3e077c09c390c7acc4649c81b1d27b&r=daafc63d9399e66689d0990a893fbddd115df89f&r=6ef1d2d9e253f74515bf89625434b605be6486ea
    # ? Revise so all tokens are considered
    # ? Built-in Function Names excluded for now
    class Tokenizer(tokens.Tokenizer):
        QUOTES = [
            "'",
            '"',
            "N'",  # unicodequote
            # ?
        ]
        COMMENTS = ["--", "//", ("/*", "*/"), ("/* !", "*/;"), ("/* !50100", "*/;")]
        IDENTIFIERS = ["`", '"']  # ?
        STRING_ESCAPES = ["\\"]

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            "INT64": TokenType.BIGINT,
            "FLOAT64": TokenType.DOUBLE,
            "BITS": TokenType.BIT,  # ? Confirm "BIT" is the same as "BITS"
            "BOTH": TokenType.BOTH,
            "BREAK": TokenType.BREAK,
            "BY": TokenType.BY,  # ? Is this keyword required? Not already added in conjunction with other keywords?
            "CASCADE": TokenType.CASCADE,
            "CATCH": TokenType.CATCH,
            "CONTAINING": TokenType.CONTAINING,
            "CURRENT": TokenType.CURRENT,
            "END_FOR": TokenType.END_FOR,
            "END_FUNCTION": TokenType.END_FUNCTION,
            "END_IF": TokenType.END_IF,
            "END_PROCEDURE": TokenType.END_PROCEDURE,
            "END_TRIGGER": TokenType.END_TRIGGER,
            "END_TRY": TokenType.END_TRY,
            "END_WHILE": TokenType.END_WHILE,
            "EXCLUSIVE": TokenType.EXCLUSIVE,
            "FOREIGN": TokenType.FOREIGN,  # ? Separate keyword from FOREIGN KEY?
            "GENERATED": TokenType.GENERATED,
            "GROUP": TokenType.GROUP,  # ? Separate keyword from GROUP BY?
            "IDENTITY": TokenType.IDENTITY,
            "INOUT": TokenType.INOUT,
            "KEY": TokenType.KEY,  # ? Separate keyword from FOREIGN KEY?
            "LEADING": TokenType.LEADING,
            "NATIONAL": TokenType.NATIONAL,
            "NCLOB": TokenType.TEXT,  # ? Seems like Clob is set to be as type TEXT, so same for NCLOB?
            # NEXT_VALUE #? NEXT VALUE FOR is already considered
            "OCTETS": TokenType.OCTETS,
            "OFF": TokenType.OFF,
            "ONLY": TokenType.ONLY,
            # ORDER #? ORDER BY is included
            "OUT": TokenType.OUT,
            # PRIMARY #? PRIMARY KEY is included generic dialect
            "RECORD_BATCHING": TokenType.RECORD_BATCHING,
            "RECORD_NUMBER": TokenType.RECORD_NUMBER,
            "RESTRICT": TokenType.RESTRICT,
            "RETURN": TokenType.RETURNING,  # ? Same as RETURNING type?
            "STARTING": TokenType.STARTING,
            "THROW": TokenType.THROW,
            "TO": TokenType.TO,
            "TRAILING": TokenType.TRAILING,
            "UNKNOWN": TokenType.UNKNOWN,
            "VAR": TokenType.VAR,  # ? Is VAR same as any of NCHAR, VARCHAR, NVARCHAR?
            "VER": TokenType.VER,
            "WHILE": TokenType.WHILE,
            "_RECORD_ID": TokenType._RECORD_ID,
            "_RECORD_PARTITIONID": TokenType._RECORD_PARTITIONID,
            "_RECORD_SEQUENCE": TokenType._RECORD_SEQUENCE,
            "_RECORD_TRANSACTION": TokenType._RECORD_TRANSACTION,
            "_UTF8": TokenType.INTRODUCER,
            "_UTF16": TokenType.INTRODUCER,
            "_UTF16LE": TokenType.INTRODUCER,
            "_UTF32": TokenType.INTRODUCER,
            "_UTF8MB3": TokenType.INTRODUCER,
            "VARCHAR2": TokenType.VARCHAR,
            "_UTF8MB4": TokenType.INTRODUCER,
        }

    class Parser(parser.Parser):
        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,
            TokenType.LOCK: lambda self: self._parse_lock_table(),
            TokenType.INSERT: lambda self: self._parse_insert(),
        }

        def _parse_lock_table(self) -> exp.ExclusiveLock:
            self._match(TokenType.LOCK)
            lock = self._prev.text.upper()
            table = None
            if self._match(TokenType.TABLE):
                table = self._prev.text.upper()
            tbl_name = self._parse_id_var()
            exclusive = None
            if self._match(TokenType.EXCLUSIVE):
                exclusive = self._prev.text.upper()
            return self.expression(
                exp.ExclusiveLock, this=lock, kind=table, tbl_name=tbl_name, lock_type=exclusive
            )

    class Generator(generator.Generator):
        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ColumnDef: transforms.preprocess([_auto_increment_to_generated_by_default]),
            exp.Create: replace_db_to_schema,
            exp.ColumnConstraint: transforms.preprocess([_remove_collate]),
            exp.Properties: no_properties_sql,
            exp.CommentColumnConstraint: no_comment_column_constraint_sql,
            exp.AddConstraint: _parse_foreign_key_index,
            exp.Constraint: _parse_foreign_key_index,
            exp.ForeignKey: _parse_fk,
            exp.SpatialKey: _parse_spatial_key,
            # exp.UniqueColumnConstraint: _parse_unique,
            exp.FullTextKey: _parse_fulltext_key,
            # exp.Introducer: _parse_introducer,
        }
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.MEDIUMINT: "INTEGER",  # ? Confirm NUMBER is most appropriate, and not
            exp.DataType.Type.TINYBLOB: "BLOB",  # ? Confirm NUMBER is most appropriate, and not
            exp.DataType.Type.TINYTEXT: "VARCHAR(255)",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.JSON: "TEXT",
            exp.DataType.Type.VARBINARY: "BLOB",
            exp.DataType.Type.POINT: "TEXT",
            exp.DataType.Type.INT_UNSIGNED: "BIGINT",
            exp.DataType.Type.SMALLINT_UNSIGNED: "SMALLINT",
            exp.DataType.Type.BIGINT_UNSIGNED: "BIGINT",
            exp.DataType.Type.TINYINT_UNSIGNED: "INTEGER",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.VARCHAR: "VARCHAR"

            # ? Revise below and add
            # exp.DataType.Type.TINYINT: "INT64",
            # exp.DataType.Type.SMALLINT: "INT64",
            # exp.DataType.Type.INT: "INT64",
            # exp.DataType.Type.BIGINT: "INT64",
            # exp.DataType.Type.DECIMAL: "NUMERIC",
            # exp.DataType.Type.FLOAT: "FLOAT64",
            # exp.DataType.Type.DOUBLE: "FLOAT64",
            # exp.DataType.Type.BOOLEAN: "BOOL",
            # exp.DataType.Type.TEXT: "STRING",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.EngineProperty: exp.Properties.Location.UNSUPPORTED,
            exp.CharacterSetProperty: exp.Properties.Location.UNSUPPORTED,
            exp.CollateProperty: exp.Properties.Location.UNSUPPORTED,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
            exp.PartitionedByProperty: exp.Properties.Location.POST_EXPRESSION,
        }

        def partitionedbyproperty_sql(self, expression: exp.PartitionedByProperty) -> str:
            type = expression.args["type"]
            subpart_exp = expression.args["subpart_exp"]
            main_partition = expression.args["main_partition"]
            if expression.args["subpartition"] and subpart_exp:
                subpart_sql = ",\n".join(subpart_exp)
                subpart_sql = f"\n{subpart_sql}\n"
            else:
                subpart_sql = ""
            return f"PARTITION BY {type} ({main_partition}) ({subpart_sql})"

        def exclusivelock_sql(self, expression: exp.ExclusiveLock) -> str:
            kind = self.sql(expression, "kind")
            kind = "TABLE" if kind == "TABLES" else ""
            tbl_name = self.sql(expression, "tbl_name")
            tbl_name = f"{tbl_name}" if tbl_name else ""
            lock_type = self.sql(expression, "lock_type")
            lock_type = " EXCLUSIVE" if lock_type in ["WRITE", "READ"] else ""
            return f"LOCK {kind} {tbl_name}{lock_type}"

        def keycolumnconstraintforindex_sql(
            self, expression: exp.KeyColumnConstraintForIndex
        ) -> str:
            expression.args.get("expression")
            expression.args.get("desc")
            key_name = expression.args.get("keyname")
            col_name = expression.args.get("colname")
            opts = expression.args.get("options")
            if opts is False and not None:
                return f"KEY {key_name} {col_name} {opts}"
            else:
                return f"KEY {key_name} {col_name}"

        def datatype_sql(self, expression: exp.DataType) -> str:
            if expression.is_type("bit"):
                expression = expression.copy()
                expression.set("this", exp.DataType.Type.BOOLEAN)
                expression.args["expressions"] = None
            if expression.is_type("tinyint"):
                expression = expression.copy()
                expression.set("this", exp.DataType.Type.INT)
                expression.args["expressions"] = None
            if expression.is_type("int"):
                expression = expression.copy()
                expression.set("this", exp.DataType.Type.INT)
                expression.args["expressions"] = None
            if expression.is_type("set"):
                raise UnsupportedError("SET COLUMN CONSTRAINT is not supported in NuoDB")

            return super().datatype_sql(expression)

        # ? Should all of these datatypes that NuoDB doesn't support be popped?
        # TYPE_MAPPING.pop(exp.DataType.Type.BIGDECIMAL)
        # TYPE_MAPPING.pop(exp.DataType.Type.BIGSERIAL)
        # TYPE_MAPPING.pop(exp.DataType.Type.DATETIME)
        # TYPE_MAPPING.pop(exp.DataType.Type.DATETIME64)
        # TYPE_MAPPING.pop(exp.DataType.Type.ENUM)
        # TYPE_MAPPING.pop(exp.DataType.Type.INT4RANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.INT4MULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.INT8RANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.INT8MULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.NUMRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.NUMMULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.TSRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.TSMULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.TSTZRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.TSTZMULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.DATERANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.DATEMULTIRANGE)
        # TYPE_MAPPING.pop(exp.DataType.Type.DECIMAL)
        # TYPE_MAPPING.pop(exp.DataType.Type.GEOGRAPHY)
        # TYPE_MAPPING.pop(exp.DataType.Type.GEOMETRY)
        # TYPE_MAPPING.pop(exp.DataType.Type.HLLSKETCH)
        # ? OTHERS TO BE ADDED
