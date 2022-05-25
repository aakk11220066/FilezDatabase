from Solution import *

from typing import List
from Utility.Status import Status
from Utility.DBConnector import DBConnector
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk


conn = DBConnector()

fileID = 123
file_type = "png"
file = File(fileID, type=file_type, size=100)
diskID = 123
disk = Disk(diskID, company="kivcorp", speed=15, free_space=500, cost=30)
ramID = 123
ram = RAM(ramID, company="kivcorp", size=300)

# ----------------------------------------


createTables()

# ----------------------------------------

clearTables()

# ----------------------------------------

#dropTables()

# ----------------------------------------


addFile(file)


# ----------------------------------------


getFileByID(fileID)

# ----------------------------------------


deleteFile(file)

# ----------------------------------------


addDisk(disk)

# ----------------------------------------


getDiskByID(diskID)

# ----------------------------------------


deleteDisk(diskID)

# ----------------------------------------


addRAM(ram)

# ----------------------------------------


getRAMByID(ramID)

# ----------------------------------------


deleteRAM(ramID)

# ----------------------------------------


addDiskAndFile(disk, file)

# ----------------------------------------


addFileToDisk(file, diskID)

# ----------------------------------------


removeFileFromDisk(file, diskID)

# ----------------------------------------

addRAMToDisk(ramID, diskID)

# ----------------------------------------


removeRAMFromDisk(ramID, diskID)

# ----------------------------------------


averageFileSizeOnDisk(diskID)

# ----------------------------------------


diskTotalRAM(diskID)

# ----------------------------------------


getCostForType(type=file_type)

# ----------------------------------------


getFilesCanBeAddedToDisk(diskID)

# ----------------------------------------


getFilesCanBeAddedToDiskAndRAM(diskID)

# ----------------------------------------


isCompanyExclusive(diskID)

# ----------------------------------------


getConflictingDisks()

# ----------------------------------------


mostAvailableDisks()

# ----------------------------------------


getCloseFiles(fileID)

dropTables()
exit()
