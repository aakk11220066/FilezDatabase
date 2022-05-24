from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


conn = Connector.DBConnector()

# ----------------------------------------

def create_entity(name, attributes):
    attr_list = str(attributes)[1:-1]
    conn.execute(f"CREATE TABLE public.{name}({attr_list});")

def create_entities():
    create_entity("file", (
        'fileID     integer     NOT NULL    CHECK (fileID > 0)',
        'type       text        NOT NULL',
        'size       integer     NOT NULL    CHECK (size >= 0)'
    ))
    create_entity("disk", (
        'diskID     integer     NOT NULL    CHECK (diskID > 0)',
        'company    text        NOT NULL',
        'speed      integer     NOT NULL    CHECK (speed > 0)',
        'free_space integer     NOT NULL    CHECK (free_space >= 0)',
        'cost       integer     NOT NULL    CHECK (cost > 0)'
    ))
    create_entity("ram", (
        'ramID      integer     NOT NULL    CHECK (ramID > 0)',
        'company    text        NOT NULL',
        'size       integer     NOT NULL    CHECK (size > 0)'
    ))

def create_many2one_relation(name, src, tgt):
    conn.execute(f"
        CREATE TABLE public.{name}(
            {src} integer,
            {tgt} integer,
            PRIMARY KEY ({src})
            CONSTRAINT src_foreign_constraint FOREIGN KEY ({src})
                REFERENCES public.{src} ({src}ID) MATCH SIMPLE
                ON UPDATE CASCADE
                ON DELETE CASCADE,
            CONSTRAINT tgt_foreign_constraint FOREIGN KEY ({tgt})
                REFERENCES public.{tgt} ({tgt}ID) MATCH SIMPLE
                ON UPDATE CASCADE
                ON DELETE CASCADE,
            CONSTRAINT disksize_nonnegative CHECK (size >= 0)
        );
    ")

def create_relations():
    create_many2one_relation("file_on_disk", src='file', tgt='disk')
    create_many2one_relation("ram_on_disk", src='ram', tgt='disk')

def createTables():
    create_entities()
    create_relations()

# ----------------------------------------

def clear_table(name):
    conn.execute(f"DELETE FROM {name}")

def clearTables():
    clear_table("file")
    clear_table("ram")
    clear_table("disk")

# ----------------------------------------

def drop_table(name):
    conn.execute(f"DROP TABLE {name}")

def dropTables():
    drop_table("file")
    drop_table("disk")
    drop_table("ram")

# ----------------------------------------


def addFile(file: File) -> Status:
    try:
        conn.execute(f"
            INSERT INTO public.file ('fileID', 'type', 'size')
            VALUES({file.getFileID()},{file.getType()},{file.getSize()});
        ")
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
    try:
         num_results, selected_files = conn.execute(f"
            SELECT * FROM public.file WHERE fileID={fileID}
        ")
        if num_results != 1:
            return File.badFile()
        file_attributes = selected_files[0]
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return File.badFile()
    return File(**file_attributes)

# ----------------------------------------


def deleteFile(file: File) -> Status:
    try:
        num_deleted, _ = conn.execute(f"
            conn.execute("
                DELETE FROM public.file
                WHERE fileID={file.fileID}
            ")
        ")
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Status.ERROR

    # TODO: subtract file.size from the disk it's on, if it is on a disk.
    # Perhaps just store disk size and make free_space a view instead?
    return Status.OK

# ----------------------------------------


def addDisk(disk: Disk) -> Status:
    try:
        conn.execute("
            INSERT INTO public.disk ('diskID', 'company', 'speed', 'free_space', 'cost')
            VALUES({disk.getDiskID()},{disk.getCompany()},{disk.getSpeed()},{dist.getFreeSpace()},{disk.getCost()});
        ")
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
    try:
         num_results, selected_disks = conn.execute(f"
            SELECT * FROM public.disk WHERE diskID={diskID}
        ")
        if num_results != 1:
            return Disk.badDisk()
        disk_attributes = selected_disks[0]
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Disk.badDisk()
    return Disk(**disk_attributes)

# ----------------------------------------


def deleteDisk(diskID: int) -> Status:
    try:
        num_deleted, _ = conn.execute(f"
            conn.execute("
                DELETE FROM public.disk
                WHERE diskID={disk.diskID}
            ")
        ")
        if num_deleted == 0:
            return Status.NOT_EXISTS
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Status.ERROR

    return Status.OK

# ----------------------------------------


def addRAM(ram: RAM) -> Status:
    try:
        conn.execute("
            INSERT INTO public.ram ('ramID', 'company', 'size')
            VALUES({ram.getRamID()},{ram.getCompany()},{ram.getSize()});
        ")
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
    try:
         num_results, selected_rams = conn.execute(f"
            SELECT * FROM public.ram WHERE ramID={ramID}
        ")
        if num_results != 1:
            return RAM.badRam()
        ram_attributes = selected_rams[0]
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return RAM.badRam()
    return RAM(**ram_attributes)

# ----------------------------------------


def deleteRAM(ramID: int) -> Status:
    try:
        num_deleted, _ = conn.execute(f"
            conn.execute("
                DELETE FROM public.ram
                WHERE ramID={file.ramID}
            ")
        ")
        if num_deleted == 0:
            return Status.NOT_EXISTS
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)  # FIXME: DELETEME
        return Status.ERROR

    return Status.OK

# ----------------------------------------


def addDiskAndFile(disk: Disk, file: File) -> Status:
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
