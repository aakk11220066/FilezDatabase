from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


# ----------------------------------------

def get_create_entity_cmd(name, attributes):
    attr_list = str(attributes)[1:-1].replace("'","")
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

def get_create_many2one_relation_cmd(name, src, tgt):
    return f" \
            CREATE TABLE public.{name}( \
                {src}ID integer, \
                {tgt}ID integer, \
                PRIMARY KEY ({src}ID), \
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
    return get_create_many2one_relation_cmd("file_on_disk", src='file', tgt='disk') + \
        get_create_many2one_relation_cmd("ram_on_disk", src='ram', tgt='disk')

def createTables():
    conn = Connector.DBConnector()
    cmd = get_create_entities_cmd() + \
        get_create_relations_cmd()
    conn.execute(f"BEGIN; {cmd} COMMIT;")

# ----------------------------------------

def get_clear_table_cmd(name):
    return f"DELETE FROM {name} CASCADE; "

def clearTables():
    conn = Connector.DBConnector()
    cmd = get_clear_table_cmd("file") + \
        get_clear_table_cmd("ram") + \
        get_clear_table_cmd("disk") + \
        get_clear_table_cmd("file_on_disk") + \
        get_clear_table_cmd("ram_on_disk")
    conn.execute(f"BEGIN; {cmd} COMMIT;")

# ----------------------------------------

def get_drop_table_cmd(name):
    return f"DROP TABLE {name} CASCADE; "

def dropTables():
    conn = Connector.DBConnector()
    cmd = get_drop_table_cmd("file") + \
        get_drop_table_cmd("disk") + \
        get_drop_table_cmd("ram") + \
        get_drop_table_cmd("file_on_disk") + \
        get_drop_table_cmd("ram_on_disk")
    conn.execute(f"BEGIN; {cmd} COMMIT;")

# ----------------------------------------


def addFile(file: File) -> Status:
    conn = Connector.DBConnector()
    try:
        cmd = f"INSERT INTO public.file ('fileID', 'type', 'size') \
            VALUES({file.getFileID()},{file.getType()},{file.getSize()});"
        conn.execute(f"BEGIN; {cmd} COMMIT;")
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        return Status.BAD_PARAMS  # in case of illegal parameters.
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS   # if a file with the same ID already exists. *
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Status.ERROR  # in case of a database error
    return Status.OK

# ----------------------------------------


def getFileByID(fileID: int) -> File:
    conn = Connector.DBConnector()
    try:
        cmd = f"SELECT * FROM public.file  \
            WHERE fileID={fileID};"
        num_results, selected_files = conn.execute(f"BEGIN; {cmd} COMMIT;")
        if num_results != 1:
            return File.badFile()
        file_attributes = selected_files[0]
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return File.badFile()
    return File(**file_attributes)

# ----------------------------------------


def deleteFile(file: File) -> Status:
    conn = Connector.DBConnector()
    try:
        cmd = f"DELETE FROM public.file \
            WHERE fileID={file.fileID};"
        # TODO: subtract file size from disk free_space if file is on disk \
        num_deleted, _ = conn.execute("BEGIN; {cmd} COMMIT;")
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Status.ERROR

    return Status.OK

# ----------------------------------------


def addDisk(disk: Disk) -> Status:
    conn = Connector.DBConnector()
    try:
        cmd = f"INSERT INTO public.disk ('diskID', 'company', 'speed', 'free_space', 'cost') \
                VALUES({disk.getDiskID()},{disk.getCompany()},{disk.getSpeed()},{dist.getFreeSpace()},{disk.getCost()});"
        conn.execute("BEGIN; {cmd} COMMIT;")
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        return Status.BAD_PARAMS  # in case of illegal parameters.
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS   # if a disk with the same ID already exists. *
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Status.ERROR  # in case of a database error
    return Status.OK

# ----------------------------------------


def getDiskByID(diskID: int) -> Disk:
    conn = Connector.DBConnector()
    try:
        cmd = f"SELECT * FROM public.disk \
            WHERE diskID={diskID};"
        num_results, selected_disks = conn.execute("BEGIN; {cmd} COMMIT;")
        if num_results != 1:
            return Disk.badDisk()
        disk_attributes = selected_disks[0]
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Disk.badDisk()
    return Disk(**disk_attributes)

# ----------------------------------------


def deleteDisk(diskID: int) -> Status:
    conn = Connector.DBConnector()
    try:
        cmd = f"DELETE FROM public.disk \
            WHERE diskID={disk.diskID}"
        num_deleted, _ = conn.execute("BEGIN; {cmd} COMMIT;")
        if num_deleted == 0:
            return Status.NOT_EXISTS
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Status.ERROR

    return Status.OK

# ----------------------------------------


def addRAM(ram: RAM) -> Status:
    conn = Connector.DBConnector()
    try:
        cmd = f"INSERT INTO public.ram ('ramID', 'company', 'size') \
                VALUES({ram.getRamID()},{ram.getCompany()},{ram.getSize()});"
        conn.execute("BEGIN; {cmd} COMMIT;")
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        return Status.BAD_PARAMS  # in case of illegal parameters.
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS   # if a disk with the same ID already exists. *
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Status.ERROR  # in case of a database error
    return Status.OK

# ----------------------------------------


def getRAMByID(ramID: int) -> RAM:
    conn = Connector.DBConnector()
    try:
        cmd = f"SELECT * FROM public.ram \
            WHERE ramID={ramID};"
        num_results, selected_rams = conn.execute("BEGIN; {cmd} COMMIT;")
        if num_results != 1:
            return RAM.badRam()
        ram_attributes = selected_rams[0]
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return RAM.badRam()
    return RAM(**ram_attributes)

# ----------------------------------------


def deleteRAM(ramID: int) -> Status:
    conn = Connector.DBConnector()
    try:
        cmd = f"DELETE FROM public.ram \
            WHERE ramID={ram.ramID};"
        num_deleted, _ = conn.execute("BEGIN; {cmd} COMMIT;")
        if num_deleted == 0:
            return Status.NOT_EXISTS
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Status.ERROR

    return Status.OK

# ----------------------------------------


def addDiskAndFile(disk: Disk, file: File) -> Status:
    conn = Connector.DBConnector()
    try:
        cmd = f"\
            INSERT INTO public.disk ('diskID', 'company', 'speed', 'free_space', 'cost') \
                VALUES({disk.getDiskID()},{disk.getCompany()},{disk.getSpeed()},{dist.getFreeSpace()},{disk.getCost()}); \
            INSERT INTO public.file ('fileID', 'type', 'size') \
                VALUES({file.getFileID()},{file.getType()},{file.getSize()});"
        conn.execute("BEGIN; {cmd} COMMIT;")
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        return Status.BAD_PARAMS  # in case of illegal parameters.
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS   # if a disk with the same ID already exists. *
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Status.ERROR  # in case of a database error

    return Status.OK

# ----------------------------------------


def addFileToDisk(file: File, diskID: int) -> Status:
    return Status.OK

# ----------------------------------------


def removeFileFromDisk(file: File, diskID: int) -> Status:
    return Status.OK

# ----------------------------------------


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    return Status.OK

# ----------------------------------------


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    return Status.OK

# ----------------------------------------


def averageFileSizeOnDisk(diskID: int) -> float:
    return 0

# ----------------------------------------


def diskTotalRAM(diskID: int) -> int:
    return 0

# ----------------------------------------


def getCostForType(type: str) -> int:
    return 0

# ----------------------------------------


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []

# ----------------------------------------


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []

# ----------------------------------------


def isCompanyExclusive(diskID: int) -> bool:
    return True

# ----------------------------------------


def getConflictingDisks() -> List[int]:
    return []

# ----------------------------------------


def mostAvailableDisks() -> List[int]:
    return []

# ----------------------------------------


def getCloseFiles(fileID: int) -> List[int]:
    return []
