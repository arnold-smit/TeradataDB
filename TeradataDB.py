import os
import csv
import pyodbc
import uuid
import re
import pandas as pd
from subprocess import call

# -------------------------------------------------------------------------------------
# Helper Functions To Support TPT
# -------------------------------------------------------------------------------------


def map_type_pandas_to_td(col_name, col_val):
    # try:
    #    col_type = str(type(next(e for e, ok in zip(col_val, pd.notnull(col_val)) if ok))).lower()
    # except StopIteration:  # when column is empty as defined by notnull --> use dtype
    #    col_type = str(col_val.dtype).lower()
    #
    col_type = str(col_val.dtype).lower()
    if "int64" in col_type:
        return f'"{col_name}" bigint'
    elif "int8" in col_type:
        return f'"{col_name}" smallint'
    elif "int16" in col_type:
        return f'"{col_name}" smallint'
    elif "int" in col_type:
        return f'"{col_name}" integer'
    elif "long" in col_type:
        return f'"{col_name}" bigint'
    elif "float32" in col_type:
        return f'"{col_name}" real'
    elif "float64" in col_type:
        return f'"{col_name}" double precision'
    elif "float" in col_type:
        return f'"{col_name}" double precision'
    elif "datetime" in col_type:
        return f'"{col_name}" timestamp'
    elif "date" in col_type:
        return f'"{col_name}" date'
    elif "time" in col_type:
        return f'"{col_name}" time'
    elif "category" in col_type:
        max_cat_len = max([len(str(cat)) for cat in col_val.cat.categories])
        return f'"{col_name}" varchar({max_cat_len})'
    elif "str" in col_type:
        return f'"{col_name}" varchar({col_val.str.len().max()})'
    elif "unicode" in col_type:
        return f'"{col_name}" varchar({col_val.str.len().max()})'
    elif "object" in col_type:
        ## dates have datetime.dt.date have dtype object - need to check 50 to see if they all follow the date format
        nr = min((~col_val.isna()).sum(), 50)
        if (
            sum(
                [
                    1
                    for d in col_val[~col_val.isna()][:nr]
                    if re.fullmatch(r"^\d{4}-[01]\d-[0123]\d$", str(d))
                ]
            )
            == nr
        ):
            return f'"{col_name}" date'
        else:
            return f'"{col_name}" varchar({col_val.str.len().max()})'
    else:
        raise Exception("No valid type conversion available for {}".format(col_type))


def map_td_column_meta_data(df):
    tpe_dict = {
        "TS": lambda cln, cdi, cfr: (f"timestamp", "datetime"),
        "DA": lambda cln, cdi, cfr: (f"ansidate", "date"),
        "CV": lambda cln, cdi, cfr: (f"varchar({cln})", "string"),
        "CF": lambda cln, cdi, cfr: (f"char({cln})", "string"),
        "I1": lambda cln, cdi, cfr: (f"byteint", "Int8"),
        "I2": lambda cln, cdi, cfr: (f"smallint", "Int16"),
        "I ": lambda cln, cdi, cfr: (f"int", "Int32"),
        "I8": lambda cln, cdi, cfr: (f"bigint", "Int64"),
        "D ": lambda cln, cdi, cfr: (f"decimal({int(cdi)},{int(cfr)})", "float64"),
        "F ": lambda cln, cdi, cfr: (f"float", "float64"),
    }
    return pd.DataFrame(
        [
            (cname.lower(), *tpe_dict[ctype](clength, cdigits, cfraction))
            for _, (cname, ctype, clength, cdigits, cfraction) in df.iterrows()
        ],
        columns=["name", "td_type", "pandas_type"],
    )


# -------------------------------------------------------------------------------------
# Teradata: thin wrapper around pyodbc
# -------------------------------------------------------------------------------------
class TeradataDB(object):
    """Class to interface with Teradata"""

    def __reset_connection(self):
        """Reset the connection."""
        self.__cnxn = pyodbc.connect("dsn={}".format(self.__dsn), autocommit=True)

    def __reset_cursor(self):
        """Reset cursor"""
        self.__curs.close()
        self.__curs = self.__cnxn.cursor()

    def __reset(self):
        """Reset both the connection and the cursor."""
        self.__reset_connection()
        self.__reset_cursor()

    def __handle_exception(self, err):
        """Internal method, used to reset the connection in case of certain exceptions. NEEDS UPDATING USES ASTER ERROR MSG's."""
        err_flat = "\n".join(err.args)
        print(err_flat)
        if "Database connection broken" in err_flat:
            self.__reset()
        elif "queries ignored until end of transaction block" in err_flat:
            self.__reset_cursor()
        elif "closed cursor" in err_flat:
            self.__reset_cursor()
        else:
            raise err

    def __print_sql(self, sql):
        """Internal method used to pretty print the query sql."""
        sqll = sql.split("\n")
        leadl = min([len(l) - len(l.lstrip()) for l in sqll])
        sqll = "\n".join([l[leadl:] for l in sqll])
        print(sqll)

    def __call_db(self, fun, args, nretry=2):
        """Internal method used to make the actual database call. Set to retry in case of an exception (mainly connection closed by DB)."""
        if self.__verbose:
            self.__print_sql(args[0])
        if self.__dryrun:
            return None
        lst_err_msg = ""
        for _ in range(nretry):
            try:
                if type(args) == tuple:
                    return fun(*args)
                elif type(args) == str:
                    return fun(args)
                else:
                    raise Exception(
                        "Error in Teradata.__call_db method - args must be a str or tuple!"
                    )
            except Exception as err:
                lst_err_msg = err
                continue
        self.__handle_exception(lst_err_msg)

    def __init__(self, dsn, verbose=False, dryrun=False):
        """Init method: uses ODBC connection with dsn name dsn, and sets the verbose (print queries prior to execution) & dryrun (prevents queries to execution) arguments."""
        self.__user = (
            os.getenv("USERNAME")
            if os.getenv("USERNAME") != None
            else os.getenv("USER")
        ).upper()
        self.__dsn = dsn
        self.__cnxn = pyodbc.connect("dsn={}".format(self.__dsn), autocommit=True)
        self.__default_schema = self.__cnxn.getinfo(pyodbc.SQL_DATABASE_NAME).lower()
        self.__curs = self.__cnxn.cursor()
        self.__verbose = verbose
        self.__dryrun = dryrun

    def set_verbose(self, verbose=True):
        """Set verbose to True to print the queries prior to execution"""
        self.__verbose = verbose

    def set_dryrun(self, dryrun=True):
        """Set dryrun to True to prevent the queries from getting executed. Combined with verbose=True, this is usefull to print all queries in order of execution."""
        self.__dryrun = dryrun

    def exec_sql(self, sql):
        """Execute the SQL query <<argument 1>> (if the query generates output -> use method get_name)."""
        self.__call_db(self.__curs.execute, sql, 2)

    def get_data(self, sql):
        """Execute the query sql and return the result as a Pandas DataFrame."""
        return self.__call_db(pd.read_sql, (sql, self.__cnxn), 2)

    ## use fastextract tool to pull data from teradata
    def tpt_import(self, sql, dir_name, base_name, sep="\x07"):
        """Extract result of query sql on Teradata into file file_name on AAP using TPT FastExport.
           When argument sql does not contain 'select ', but instead contains a string of the from schema.table, then the table is imported.
           By default the data is stored as a delimited file, using sep (default='\x07').
           When output format is DLM, the argument generate_read_code=True, determines if a snippet of python code is returned to read in the data.
        """
        print(f"1. determine the schema")
        sql = re.sub(r"\s+", " ", re.sub("--.*", "", sql).replace("\n", " "))
        if "select " not in sql:
            sql = f"select * from {sql}"
        qry_schema = re.sub(r"top\s+\d+", "", sql, flags=re.IGNORECASE)
        self.drop_table(f"{self.__default_schema}.tpt_import_schema")
        self.exec_sql(
            f"create table {self.__default_schema}.tpt_import_schema as (\n{qry_schema}\n) with no data;"
        )
        df_col_td = self.get_data(
            f"select columnname,\n"
            + f"       columntype,\n"
            + f"       case when (columnformat like 'X(%') then regexp_substr(columnformat, '\\d+', 1) else columnlength end as columnlength,\n"
            + f"       decimaltotaldigits,\n"
            + f"       decimalfractionaldigits\n"
            + f"from   dbc.columns\n"
            + f"where  lower(databasename) = '{self.__default_schema}' and\n"
            + f"       lower(tablename) = 'tpt_import_schema'\n"
            + f"order  by columnid;"
        )
        df_col_meta = map_td_column_meta_data(df_col_td)
        self.drop_table(f"{self.__default_schema}.tpt_import_schema")
        ##
        print(f"2. generate script")
        out_file = os.path.join(dir_name, base_name + ".dlm")
        out_tpt = os.path.join(os.getcwd(), uuid.uuid1().hex + ".tpt")
        tpt_script = open(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "TeradataDBImport.tpt"
            ),
            "r",
        ).read()
        tpt_script = tpt_script.replace(
            "<<schema>>",
            ", ".join(
                [
                    f"{name.strip()} {tdtype.strip()}"
                    for _, (name, tdtype, _) in df_col_meta.iterrows()
                ]
            ),
        )
        tpt_script = tpt_script.replace("<<query>>", sql.replace("'", "''"))
        tpt_script = tpt_script.replace("<<dest_file>>", out_file)
        tpt_script = tpt_script.replace("<<sep>>", f'{hex(ord(sep)).split("x")[1]:>02}')
        with open(out_tpt, "w") as f:
            f.write(tpt_script)
        ##
        print(f"3. call tbuild to pull the data - this might take some time ... ")
        ret_val = call(["tbuild", "-f", out_tpt])
        if ret_val == 0:
            rows_downloaded = sum(1 for line in open(out_file))
            print(f"SUCCESS: loaded {rows_downloaded} records!")
            os.remove(out_tpt)
            print("4. generate python code to import delimited file:")
            ## deal with dates
            col_names = [n.strip() for _, (n, _, t) in df_col_meta.iterrows()]
            col_types = {
                n.strip(): t.strip() if ("date" not in t) else "string"
                for _, (n, _, t) in df_col_meta.iterrows()
            }
            col_dates = [
                n.strip() for _, (n, _, t) in df_col_meta.iterrows() if "date" in t
            ]
            date_syntx = (
                ""
                if len(col_dates) == 0
                else f"    parse_dates={str(col_dates)},\n    date_parser=pd.to_datetime, ## assumes: import pandas as pd\n"
            )
            ## generate read script
            print(
                f"{base_name} = pd.read_csv('{out_file}',\n"
                + f"    delimiter='{sep}',\n"
                + f"    names={str(col_names)},\n"
                + f"    dtype={str(col_types)},\n"
                + date_syntx
                + f"    encoding='ascii'\n)"
            )
        else:
            print(f"FAILED... (leaving script) please see:")
            print(f"- script: {out_tpt}!")
            print(
                f"If possible fix tpt script and run from command: tbuild -f {out_tpt}"
            )

    ## write the schema used to upload the dataframe to Teradata. Note, since the source is a text file all fields are varchar(...)
    def __tpt_generate_schema_from_dataframe(self, df):
        cols = []
        for col in df.columns:
            ## get the max length of the columns when converted to string
            col_len = df[col].apply(lambda x: str(x)).dropna().str.len().max()
            ## just add a little for
            cols.append(f'"{col}" varchar({col_len})')
        return ", ".join(cols)

    ## write the schema used to upload the dataframe to Teradata. Note, since the source is a text file all fields are varchar(...)
    def __tpt_create_table(self, df, table_name):
        self.drop_table(table_name)
        vardefs = ", ".join([map_type_pandas_to_td(col, df[col]) for col in df.columns])
        self.exec_sql(f"create table {table_name} ({vardefs});")
        return self.check_if_table_exists(table_name)

    ## use fastload tool to pump data into teradata
    def tpt_export(self, df, schema_name, table_name):
        """Write Pandas dataFrame to delimited file, create an empty table on Teradata, & use TPT to load data into Teradata."""
        ## note: the schema is down to the source file, here it's a UTF8 file sep=\x07, so datatypes need to be varchar(...)
        ## the destination table can use most formats ...
        ## 1. generate temp filenames
        tmp_base = uuid.uuid1().hex
        out_tpt = os.path.join(os.getcwd(), tmp_base + ".tpt")
        out_dat = os.path.join(os.getcwd(), tmp_base + ".dlm")
        ## 2. write to delimited file
        print(f"1. writing data to temporary delimited file {out_dat}")
        df.to_csv(
            out_dat,
            index=False,
            header=False,
            sep="\x07",
            encoding="utf-8",
            date_format="%Y-%m-%d %H:%M:%S",
            quoting=csv.QUOTE_NONE,
        )
        ## 3. read TeradataDBLoad.tpt
        print(f"2. generating script {out_tpt}")
        tpt_script = open(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "TeradataDBExport.tpt"
            ),
            "r",
        ).read()
        ## 4. create specific tpt file
        tpt_script = tpt_script.replace(
            "<<schema>>", self.__tpt_generate_schema_from_dataframe(df)
        )
        tpt_script = tpt_script.replace("<<src_file>>", out_dat)
        tpt_script = tpt_script.replace("<<dst_schema>>", schema_name)
        tpt_script = tpt_script.replace("<<dst_table>>", table_name)
        tpt_script = tpt_script.replace(
            "<<vars>>", ", ".join([f'"{col}"' for col in df.columns])
        )
        tpt_script = tpt_script.replace(
            "<<:vars>>", ", ".join([f':"{c}"' for c in df.columns])
        )
        with open(out_tpt, "w") as f:
            f.write(tpt_script)
        ## 5. create table on Teradata to insert the data into
        print(f"3. create table {table_name} on teradata")
        self.__tpt_create_table(df, f"{schema_name}.{table_name}")
        ## 6. call tbuild
        print(
            "4. use tbuild to copy the file to Teradata and populate the table - this might take some time ... "
        )
        ret_val = call(["tbuild", "-f", out_tpt])
        if ret_val == 0:
            rows_uploaded = self.get_data(
                f"select count(*) as nr from {schema_name}.{table_name}"
            ).nr[0]
            print(
                f"SUCCESS: loaded {rows_uploaded} records into table {schema_name}.{table_name}"
            )
            os.remove(out_tpt)
            os.remove(out_dat)
        else:
            print(f"FAILED... (leaving data and script) please see:")
            print(f"- data:   {out_dat}!")
            print(f"- script: {out_tpt}!")
            print(
                f"If possible fix tpt script and run from command: tbuild -f {out_tpt}"
            )

    def collect_statistics(self, table, cols, index=False):
        """Collect statistics on table for cols (column name or list of column names) + optionally index"""
        col = "(" + ", ".join(cols) + ")" if (type(cols) == list) else cols
        tpe = "index" if index else "column"
        self.exec_sql(f"collect statistics on {table} {tpe} {col}")

    # SELECT * FROM DBC.TABLES WHERE TABLEKIND = '<see below>'
    # A = Aggregate Function
    # B = Combined Aggregate Function and ordered analytical function
    # D = JAR
    # E = External Stored Procedure
    # F = Standard Function
    # G = Trigger
    # H = Instance or Constructor Method
    # I = Join Index
    # J = Journal
    # M = Macro
    # N = Hash Index
    # O = No Primary Index (Table)
    # P = Stored Procedure
    # Q = Queue Table
    # R = Table Function
    # S = Ordered Analytical Function
    # T = Table
    # U = User-defined data type
    # V = View
    # X = Authorization
    # Y = GLOP Set

    def check_if_table_exists(self, table_name):
        """Check if table table_name exists (table_name consists of schemaname and table name)"""
        db, tb = table_name.split(".")
        nr = self.get_data(
            f"select tableName from dbc.tables where upper(databasename) = '{db.upper()}' and upper(tablename) = '{tb.upper()}' and tablekind = 'T';"
        ).shape[0]
        return nr == 1

    def check_if_view_exists(self, view_name):
        """Check if view vname exists. Argument vname: the view name"""
        db, vw = view_name.split(".")
        nr = self.get_data(
            f"select tablename from dbc.tables where upper(databasename) = '{db.upper()}' and upper(tablename) = '{vw.upper()}' and tablekind = 'V';"
        ).shape[0]
        return nr == 1

    def drop_table(self, table_name):
        """Drop table table_name if it exists"""
        if self.check_if_table_exists(table_name):
            self.exec_sql(f"drop table {table_name}")

    def drop_view(self, view_name):
        """Drop view view_name if it exists"""
        if self.check_if_view_exists(view_name):
            self.exec_sql(f"drop view {view_name}")

    def rename_table(self, current_schema, current_name, new_name):
        """Rename table current_schema.current_name into new_name (in the same schema)"""
        self.exec_sql(
            f"rename table {current_schema}.{current_name} to {current_schema}.{new_name}"
        )

    def move_table(self, current_schema, table_name, new_schema):
        """Move table current_schema.table_name to new_schema"""
        self.exec_sql(
            f"alter table {current_schema}.{table_name} set schema {new_schema}"
        )

    def change_owner(self, current_schema, table_name, new_owner):
        """Change owner of table current_schema.current_name to new_owner (in the same schema)"""
        self.exec_sql(f"alter table {current_schema}.{table_name} owner to {new_owner}")

    def grant_access(self, table_name, access_type="all", access_group="public"):
        """Grant access access_type (SELECT, INSERT, UPDATE, DELETE, ALL) on table table_name to access_group (username, rolename, ...)"""
        self.exec_sql(f"grant {access_type} on {table_name} to {access_group}")

    def get_roles(self, grantee=None):
        """Show roles. If argument grantee == None, show roles current user."""
        if grantee == None:
            grantee = self.__user
        return self.get_data(
            f"select * from dbc.rolemembers where upper(grantee) = '{grantee.upper()}' order by whengranted;"
        )

    def get_amps(self):
        """Show #AMP's."""
        return self.get_data(f"select hashamp()+1").iloc[0, 0]

    def get_spoolspace(self, user_name):
        """Show spoolspace."""
        nr_amps = self.get_amps()
        return self.get_data(
            f"select username             as username,\n"
            + f"       spoolspace / 1024**3 as spoolspace_gb,\n"
            + f"       spoolspace_gb / {nr_amps} as spoolspace_per_amp,\n"
            + f"       profilename          as profilename\n"
            + f"from   dbc.users\n"
            + f"where  trim(upper(username)) = '{user_name.upper()}';"
        )

    def get_memory(self, schema_name, table_name=""):
        """Show memory footprint of schema schema_name and table table_name (default is None -> all tables in schema)."""
        cond_table = (
            ""
            if (table_name == "")
            else f" and trim(upper(tablename)) like '{table_name.upper()}'\n"
        )
        return self.get_data(
            f"select databasename,\n"
            + f"       tablename,\n"
            + f"       sum (currentperm)/1024**3 as current_gb\n"
            + f"from   dbc.allspace\n"
            + f"where  trim(upper(databasename)) = '{schema_name.upper()}'\n{cond_table}"
            + f"group  by 1,2\n"
            + f"order  by 3 desc;\n"
        )

    def get_indices(self, schema_name, table_name=""):
        """Show indices for table schema_name.table. If table_name is ommited, memory for all tables in the schema are returned."""
        cond_table = (
            ""
            if table_name == ""
            else f" and trim(upper(tablename)) = '{table_name.upper()}'"
        )
        return self.get_data(
            f"select tablename,\n"
            + f"       indexnumber,\n"
            + f"       columnname,\n"
            + f"       indexname,\n"
            + f"       indextype\n"
            + f"from   dbc.indices\n"
            + f"where  trim(upper(databasename)) = '{schema_name.upper()}'{cond_table}\n"
            + f"order  by 1;"
        )

    def role_has_access_right(self, role, access_right="R"):
        """Show tables to which the role has access_right (default = read 'R')."""
        return self.get_data(
            f"select distinct databasename, tablename, accessright\n"
            + f"from   dbc.userrolerights\n"
            + f"where  trim(upper(rolename)) = '{role.upper()}' and\n"
            + f"       accessright = '{access_right.upper()}'\n"
            + f"order  by 1, 2, 3;"
        )

    def get_columns(self, schema_name, table_name=""):
        """Get columns in table schema_name.table_name. If one argument is passed: it should be the full schema.table name.
           Warning, the max objectname is 30 character. Teredata @ Nationwide has not enabled the 14.10 Extended Object Names feature!
        """
        if table_name == "":
            schema_name, table_name = schema_name.split(".")
        lst1 = list(
            self.get_data(
                f"select lower(columnname) as columnname\n"
                + f"from   dbc.columns\n"
                + f"where  trim(upper(databasename)) = '{schema_name.upper()}' and\n"
                + f"       trim(upper(tablename)) = '{table_name.upper()}'\n"
                + f"order  by columnid;"
            )
            .columnname.str.strip()
            .values
        )
        if len(lst1) > 0:
            lst_max_len = [e for e in lst1 if len(e.strip()) >= 30]
            if len(lst_max_len) > 0:
                print(
                    f"WARNING: Column names of max length encountered!\nPlease check for correctness: {lst_max_len}"
                )
            return lst1
        elif (len(schema_name) > 30) | (len(table_name) > 30):
            try:
                ## could be problem with tablename > 30 characters ...
                df = self.get_data(f"select top 1 * from {schema_name}.{table_name}")
                if df.shape[0] == 1:
                    return [e.lower() for e in df.columns]
            except:
                return []
        else:
            return []

    def get_column_and_descriptions(self, schema_name, table_name=""):
        """Get columns + description if filled in the meta data for table: schema_name.table_name.
           If one argument is passed: it should be the full schema.table name.
           Warning, the max objectname is 30 character.
           Teredata @ Nationwide has not enabled the 14.10 Extended Object Names feature!
        """
        if table_name == "":
            schema_name, table_name = schema_name.split(".")
        return self.get_data(
            f"select lower(columnname) as columnname, lower(commentstring) as description\n"
            + f"from   dbc.columns\n"
            + f"where  trim(upper(databasename)) = '{schema_name.upper()}' and\n"
            + f"       trim(upper(tablename)) = '{table_name.upper()}'\n"
            + f"order  by columnid;"
        )

    def search(self, search_dict):
        """Search for DATABASE, TABLE, or COLUMN by passing in a dict of conditions.
           The conditions are expressed in terms of meta-data fields: databasename, tablename, columnname, or ownername
        """
        search_dict = {
            k.upper(): f"trim({k.lower()}name) like '{v}'"
            for k, v in search_dict.items()
        }
        if len(set(search_dict.keys()) - set(["DATABASE", "TABLE", "COLUMN"])) > 0:
            raise KeyError(
                "Key not found, must be one of: ('DATABASE', 'TABLE', 'COLUMN')"
            )
        where_cond = " and ".join(search_dict.values())
        if "COLUMN" in search_dict.keys():
            tbl = "dbc.columns"
            cnames = "lower(databasename) as databasename, lower(tablename) as tablename, lower(columnname) as columnname"
            rord = "databasename, tablename, columnid"
        elif "TABLE" in search_dict.keys():
            tbl = "dbc.tables"
            cnames = "lower(databasename) as databasename, lower(tablename) as tablename, lower(creatorname) as creatorname, createtimestamp as createtimestamp"
            rord = "databasename, createtimestamp"
        else:
            tbl = "dbc.databases"
            cnames = "lower(databasename) as databasename, lower(ownername) as ownername, createtimestamp as createtimestamp, lastaccesstimestamp as lastaccesstimestamp, lastaltertimestamp as lastaltertimestamp"
            rord = "databasename, createtimestamp"
        return self.get_data(
            f"select {cnames} from {tbl} where {where_cond} order by {rord}"
        )

    def show_db_path(self, db):
        df = self.get_data(
            f"select c1.child as child, c1.parent as parent, count(*) lvl\n"
            + f"from   dbc.childrenv c1 join\n"
            + f"       dbc.childrenv c2 on c2.child = c1.parent\n"
            + f"where  upper(c1.child) like '{db.upper()}'\n"
            + f"group  by 1,2\n"
            + f"order  by 3 asc"
        )
        if df.shape[0] >= 1:
            return " >> ".join(list(df.parent.values) + [df.child[0]])
        else:
            return f"Could not find database: {db}"

    def kill_sessions(self):
        """Kill all sessions, last resort option to kill all your Teradata sessions."""
        self.exec_sql(
            f"select syslib.abortsessions (-1, '{self.__user}', 0, 'Y', 'Y');"
        )

    def monitor_sessions(self):
        """Get some info on the users open sessions."""
        return self.get_data(
            f"select username, userid, hostid, sessionno, logontime, "
            + f"reqstarttime, reqspool, reqcpu, "
            + f"ampstate, ampcpusec, ampio, "
            + f"blk1hostid, blk1sessno, blk1userid\n"
            f"from   syslib.MonitorMySessions() _;"
        )

