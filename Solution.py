from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql
import psycopg2


# Decorators

def perform_sql_txn(cmd_constructor):
    # Send an SQL query to the server and return the result
    # Input to decorator (output of decorated function): SQL query: str
    # Output: Result of SQL query to the database
    def inner(*args, **kwargs):
        conn = Connector.DBConnector()
        cmd = cmd_constructor(*args, **kwargs)

        try:
            #print(cmd) #FIXME: DELETEME
            num_results, result = conn.execute(f"BEGIN; {cmd}")
            conn.commit()
        except Exception as e:
            conn.rollback()
            e.conn = conn
            #print(e) #FIXME: DELETEME
            raise e
        finally:
            conn.close()
        return num_results, result

    return inner


def assert_exists(sql_func):
    # Ensures at least 1 tuple was returned from sql_func, else returns Status.NOT_EXISTS
    def inner(*args, **kwargs):
        try:
            num_results, attributes = sql_func(*args, **kwargs)
        except DatabaseException.FOREIGN_KEY_VIOLATION:
            return Status.NOT_EXISTS
        if num_results == 0:
            return Status.NOT_EXISTS
        return attributes

    return inner


def assert_no_database_error(sql_func):
    # catches DatabaseException.UNKNOWN_ERROR
    def inner(*args, **kwargs):
        try:
            result = sql_func(*args, **kwargs)
        except (DatabaseException.UNKNOWN_ERROR, DatabaseException.ConnectionInvalid, psycopg2.DatabaseError) as e:
            #print(e)  # FIXME: DELETEME
            return Status.ERROR
        return result

    return inner


def return_status(sql_func):
    # Catch exceptions thrown by an SQL query and return the appropriate Status
    def inner(*args, **kwargs):
        try:
            result = sql_func(*args, **kwargs)
        except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION) as e:
            e.conn.close()
            return Status.BAD_PARAMS  # in case of illegal parameters.
        except DatabaseException.UNIQUE_VIOLATION as e:
            e.conn.close()
            return Status.ALREADY_EXISTS  # if a file/disk/ram with the same ID already exists. *
        except (DatabaseException.UNKNOWN_ERROR, DatabaseException.ConnectionInvalid, psycopg2.DatabaseError) as e:
            e.conn.close()
            #print(e)  # FIXME: DELETEME
            return Status.ERROR  # in case of a database error
        if result == Status.NOT_EXISTS:  # output overriden by assert_exists decorator
            return Status.NOT_EXISTS
        return Status.OK

    return inner


# Helper functions

def none_to_null(input, is_str=False):
    if input is None:
        return "NULL"
    return input if not is_str else f"'{input}'"


# ----------------------------------------

def get_create_entity_cmd(name, attributes):
    attr_list = str(attributes)[1:-1].replace("'", "")
    return f"CREATE TABLE public.{name}({attr_list}, \
        PRIMARY KEY({name}ID)); "


def get_create_entities_cmd():
    return get_create_entity_cmd("file", (
        'fileID     integer     NOT NULL    CHECK (fileID > 0)',
        'type       text        NOT NULL',
        'size       integer     NOT NULL    CHECK (size >= 0)'
    )) + \
           get_create_entity_cmd("disk", (
               'diskID     integer     NOT NULL    CHECK (diskID > 0)',
               'company    text        NOT NULL',
               'speed      integer     NOT NULL    CHECK (speed > 0)',
               'free_space integer     NOT NULL    CHECK (free_space >= 0)',
               'cost       integer     NOT NULL    CHECK (cost > 0)'
           )) + \
           get_create_entity_cmd("ram", (
               'ramID      integer     NOT NULL    CHECK (ramID > 0)',
               'company    text        NOT NULL',
               'size       integer     NOT NULL    CHECK (size > 0)'
           ))


def get_create_many2many_relation_cmd(name, src, tgt):
    return f" \
            CREATE TABLE public.{name}( \
                {src}ID integer, \
                {tgt}ID integer, \
                UNIQUE ({src}ID, {tgt}ID), \
                FOREIGN KEY ({src}ID) \
                    REFERENCES public.{src} ({src}ID) \
                    ON UPDATE CASCADE \
                    ON DELETE CASCADE, \
                FOREIGN KEY ({tgt}ID) \
                    REFERENCES public.{tgt} ({tgt}ID) \
                    ON UPDATE CASCADE \
                    ON DELETE CASCADE \
            ); "


def get_create_relations_cmd():
    return get_create_many2many_relation_cmd("file_on_disk", src='file', tgt='disk') + \
           get_create_many2many_relation_cmd("ram_on_disk", src='ram', tgt='disk')


def get_create_view_cmd(name, attributes, src_table):
    return f"CREATE VIEW public.{name} AS (SELECT {attributes} FROM {src_table}); "


def get_create_views_cmd():
    return get_create_view_cmd(
        "all_files_on_disk",
        "diskID, public.file_on_disk.fileID, type, size",
        "public.file INNER JOIN public.file_on_disk ON public.file.fileID=public.file_on_disk.fileID"
    ) + \
   get_create_view_cmd(
       "all_rams_on_disk",
       "diskID, public.ram_on_disk.ramID, company, size",
       "public.ram INNER JOIN public.ram_on_disk ON public.ram.ramID=public.ram_on_disk.ramID"
   )

@return_status
@perform_sql_txn
def createTables():
    return get_create_entities_cmd() + \
           get_create_relations_cmd() + \
           get_create_views_cmd()


# ----------------------------------------

def get_clear_table_cmd(name):
    return f"DELETE FROM {name} CASCADE; "

@return_status
@perform_sql_txn
def clearTables():
    return get_clear_table_cmd("file") + \
           get_clear_table_cmd("ram") + \
           get_clear_table_cmd("disk") + \
           get_clear_table_cmd("file_on_disk") + \
           get_clear_table_cmd("ram_on_disk")


# ----------------------------------------

def get_drop_table_cmd(name):
    return f"DROP TABLE {name} CASCADE; "


@return_status
@perform_sql_txn
def dropTables():
    return get_drop_table_cmd("file") + \
           get_drop_table_cmd("disk") + \
           get_drop_table_cmd("ram") + \
           get_drop_table_cmd("file_on_disk") + \
           get_drop_table_cmd("ram_on_disk")


# ----------------------------------------

@return_status
@perform_sql_txn
def addFile(file: File) -> Status:
    return f"INSERT INTO public.file (fileID, type, size) \
            VALUES({none_to_null(file.getFileID())},{none_to_null(file.getType(), True)},{none_to_null(file.getSize())});"


# ----------------------------------------

@assert_no_database_error
@assert_exists
@perform_sql_txn
def getFileAttributesByID(fileID: int):
    return f"SELECT * FROM public.file  \
        WHERE fileID={fileID};"


def getFileByID(fileID: int) -> File:
    selected_files = getFileAttributesByID(fileID)
    if type(selected_files) == Status:
        return File.badFile()
    file_attributes = selected_files[0]
    file_attributes["fileID"] = file_attributes.pop("fileid")
    return File(**file_attributes)


# ----------------------------------------

@return_status
@perform_sql_txn
def deleteFile(file: File) -> Status:
    return f" \
        UPDATE public.disk \
        SET free_space=free_space + {none_to_null(file.getSize())} \
        WHERE diskID IN ( \
            SELECT diskID FROM public.file_on_disk \
            WHERE fileID={none_to_null(file.getFileID())} \
        ); " + f" \
        DELETE FROM public.file \
        WHERE fileID={none_to_null(file.getFileID())}; "


# ----------------------------------------

@return_status
@perform_sql_txn
def addDisk(disk: Disk) -> Status:
    return f"INSERT INTO public.disk (diskID, company, speed, free_space, cost) \
        VALUES({none_to_null(disk.getDiskID())},{none_to_null(disk.getCompany(), True)},{none_to_null(disk.getSpeed())},{none_to_null(disk.getFreeSpace())},{none_to_null(disk.getCost())}); "


# ----------------------------------------

@assert_no_database_error
@assert_exists
@perform_sql_txn
def getDiskAttributesByID(diskID: int):
    return f"SELECT * FROM public.disk \
        WHERE diskID={diskID};"


def getDiskByID(diskID: int) -> Disk:
    selected_disks = getDiskAttributesByID(diskID)
    if type(selected_disks) == Status:
        return Disk.badDisk()
    disk_attributes = selected_disks[0]
    disk_attributes["diskID"] = disk_attributes.pop("diskid")
    return Disk(**disk_attributes)


# ----------------------------------------


@return_status
@assert_exists
@perform_sql_txn
def deleteDisk(diskID: int) -> Status:
    return f"DELETE FROM public.disk \
        WHERE diskID={diskID}; "


# ----------------------------------------

@return_status
@perform_sql_txn
def addRAM(ram: RAM) -> Status:
    return f"INSERT INTO public.ram (ramID, company, size) \
                VALUES({none_to_null(ram.getRamID())},{none_to_null(ram.getCompany(), True)},{none_to_null(ram.getSize())}); "


# ----------------------------------------

@assert_no_database_error
@assert_exists
@perform_sql_txn
def getRAMAttributesByID(ramID: int):
    return f"SELECT * FROM public.ram \
        WHERE ramID={ramID}; "


def getRAMByID(ramID: int) -> RAM:
    selected_rams = getRAMAttributesByID(ramID)
    if type(selected_rams) == Status:
        return RAM.badRAM()
    ram_attributes = selected_rams[0]
    ram_attributes["ramID"] = ram_attributes.pop("ramid")
    return RAM(**ram_attributes)


# ----------------------------------------

@return_status
@assert_exists
@perform_sql_txn
def deleteRAM(ramID: int) -> Status:
    return f"DELETE FROM public.ram \
        WHERE ramID={ramID}; "


# ----------------------------------------

@return_status
@perform_sql_txn
def addDiskAndFile(disk: Disk, file: File) -> Status:
    return f"\
            INSERT INTO public.disk (diskID, company, speed, free_space, cost) \
                VALUES({none_to_null(disk.getDiskID())},{none_to_null(disk.getCompany(), True)},{none_to_null(disk.getSpeed())},{none_to_null(disk.getFreeSpace())},{none_to_null(disk.getCost())}); \
            INSERT INTO public.file (fileID, type, size) \
                VALUES({none_to_null(file.getFileID())},{none_to_null(file.getType(), True)},{none_to_null(file.getSize())}); "


# ----------------------------------------

@return_status
@assert_exists
@perform_sql_txn
def addFileToDisk(file: File, diskID: int) -> Status:
    return f" \
        INSERT INTO public.file_on_disk (fileID, diskID) \
        VALUES ({none_to_null(file.getFileID())}, {diskID}); " + \
        f" \
        UPDATE public.disk \
        SET free_space=free_space - {none_to_null(file.getSize())} \
        WHERE diskID = {diskID}; "


# ----------------------------------------

@return_status
@perform_sql_txn
def removeFileFromDisk(file: File, diskID: int) -> Status:
    # modify free space of disk first (so we can check if file was on disk), then remove file from disk.  If fails, free_space modification will be rolled back as well
    return f" \
        UPDATE public.disk \
        SET free_space=free_space + {none_to_null(file.getSize())} \
        WHERE diskID={diskID} AND EXISTS ( \
            SELECT * FROM public.file_on_disk \
            WHERE diskID={diskID} AND fileID={none_to_null(file.getFileID())} \
        ); " + f" \
        DELETE FROM public.file_on_disk \
        WHERE fileID={none_to_null(file.getFileID())} AND diskID={diskID}; "


# ----------------------------------------

@return_status
@assert_exists
@perform_sql_txn
def addRAMToDisk(ramID: int, diskID: int) -> Status:
    return f" \
        INSERT INTO public.ram_on_disk (ramID, diskID) \
        SELECT * FROM ( \
            (SELECT ramID FROM public.ram WHERE ramID={ramID}) needless_alias1 \
            CROSS JOIN \
            (SELECT diskID FROM public.disk WHERE diskID={diskID}) needless_alias2 \
        ); "


# ----------------------------------------

@return_status
@assert_exists
@perform_sql_txn
def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    return f" \
        DELETE FROM public.ram_on_disk \
        WHERE ramID={ramID} AND diskID={diskID}; "


# ----------------------------------------

@assert_no_database_error
@assert_exists
@perform_sql_txn
def _averageFileSizeOnDisk(diskID: int):
    return f" \
        SELECT AVG(size) FROM public.all_files_on_disk \
        WHERE diskID = {diskID};"


def averageFileSizeOnDisk(diskID: int) -> float:
    averages = _averageFileSizeOnDisk(diskID)
    if averages == Status.ERROR:
        return -1
    if averages == Status.NOT_EXISTS:
        return 0
    return averages[0]["avg"]


# ----------------------------------------

@assert_no_database_error
@assert_exists
@perform_sql_txn
def _diskTotalRAM(diskID: int):
    return f" \
        SELECT SUM(size) FROM public.all_rams_on_disk \
        WHERE diskID = {diskID};"


def diskTotalRAM(diskID: int) -> int:
    sums = _diskTotalRAM(diskID)
    if sums == Status.ERROR:
        return -1
    if sums == Status.NOT_EXISTS:
        return 0
    return sums[0]["sum"]


# ----------------------------------------

@assert_no_database_error
@assert_exists
@perform_sql_txn
def _getCostForType(type: str):
    return f" \
        SELECT SUM(cost*size) FROM public.all_files_on_disk INNER JOIN public.disk ON public.disk.diskID=public.all_files_on_disk.diskID \
        WHERE type={none_to_null(type, True)}; "


def getCostForType(type: str) -> int:
    total_cost = _getCostForType(type)
    if total_cost == Status.ERROR:
        return -1
    if total_cost == Status.NOT_EXISTS:
        return 0
    return total_cost[0]["sum"]


# ----------------------------------------

@assert_no_database_error
@perform_sql_txn
def _getFilesCanBeAddedToDisk(diskID: int):
    return f" \
        SELECT fileID FROM public.file, (SELECT free_space FROM public.disk WHERE diskID={diskID}) disk_freespace_singleton \
        WHERE size <= free_space \
        ORDER BY fileID DESC \
        LIMIT 5; "


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    suggested_files = _getFilesCanBeAddedToDisk(diskID)
    return [suggested_files[i]["fileID"] for i in range(suggested_files.size())]


# ----------------------------------------

# note that both file and ram have a size attribute!
def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


# ----------------------------------------

@assert_no_database_error
@assert_exists
@perform_sql_txn
def _isCompanyExclusive(diskID: int):
    return f" \
        SELECT disk_singleton.company FROM (SELECT company FROM public.disk WHERE diskID={diskID}) disk_singleton  \
        WHERE disk_singleton.company =ALL ( \
            SELECT company FROM public.all_rams_on_disk \
            WHERE diskID={diskID} \
        ); "


def isCompanyExclusive(diskID: int) -> bool:
    return type(_isCompanyExclusive(diskID)) != Status  # query didn't fail on the database_error assertion nor the exists assertion


# ----------------------------------------

@assert_no_database_error
@perform_sql_txn
def _getConflictingDisks():
    return f" \
        SELECT DISTINCT file1tbl.diskID FROM public.file_on_disk AS file1tbl INNER JOIN public.file_on_disk AS file2tbl ON file1tbl.fileID = file2tbl.fileID \
        WHERE file1tbl.diskID <> file2tbl.diskID \
        ORDER BY file1tbl.diskID ASC;"

def getConflictingDisks() -> List[int]:
    conflicting_disks = _getConflictingDisks()
    return [conflicting_disks[i]["diskID"] for i in range(conflicting_disks.size())]


# ----------------------------------------

@assert_no_database_error
@perform_sql_txn
def _mostAvailableDisks():
    return f" \
        SELECT num_files_addable_to_disk.diskID FROM public.disk INNER JOIN ( \
             SELECT files_that_fit_on_disks.diskID, COUNT(*) FROM ( \
                 SELECT DISTINCT diskID, fileID FROM public.file CROSS JOIN public.disk \
                 WHERE size<=free_space \
             ) files_that_fit_on_disks \
             GROUP BY diskID \
        ) num_files_addable_to_disk ON public.disk.diskID = num_files_addable_to_disk.diskID \
        ORDER BY count DESC, speed DESC, diskID DESC \
        LIMIT 5; "

def mostAvailableDisks() -> List[int]:
    most_available_disk = _mostAvailableDisks()
    return [most_available_disk[i]["diskID"] for i in range(most_available_disk.size())]

# ----------------------------------------

@assert_no_database_error
@perform_sql_txn
def _getCloseFiles(fileID: int):
    return f" \
        SELECT fileID FROM ( \
            SELECT fileID, COUNT(*) FROM public.file_on_disk \
            WHERE diskID IN ( \
                SELECT diskID FROM public.file_on_disk \
                WHERE fileID={fileID} \
            ) \
            GROUP BY fileID \
            HAVING 2*COUNT(*) >=ALL ( \
                SELECT COUNT(*) FROM public.file_on_disk \
                WHERE fileID={fileID} \
            ) \
            ORDER BY count DESC \
            LIMIT 10 \
        ) disordered_results \
        ORDER BY count ASC \
    ;"

def getCloseFiles(fileID: int) -> List[int]:
    closest_files = _getCloseFiles(fileID)
    return [closest_files[i]["fileID"] for i in range(closest_files.size())]
