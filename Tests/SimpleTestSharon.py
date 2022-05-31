import unittest
import sys
sys.path.insert(0,"..")
import Solution
from Utility.Status import Status
from abstractTest import AbstractTest
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk

'''
    Simple test, create one of your own
    make sure the tests' names start with test_
'''


class Test(AbstractTest):
    def test_Disk(self) -> None:
        # check database error
        Solution.dropTables()
        self.assertEqual(Status.ERROR, Solution.addDisk(Disk(1, "DELL", 1, 1, 1)),
                         "ERROR in case of a database error")
        self.assertEqual(Status.ERROR, Solution.deleteDisk(1), "ERROR in case of a database error")
        Solution.createTables()

        # basic test
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 2, 10, 10)),
                         "ID 1 ALREADY_EXISTS")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 2, 10)),
                         "ID 1 ALREADY_EXISTS")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 10, 2)),
                         "ID 1 ALREADY_EXISTS")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 2, 10)),
                         "DISK ALREADY EXISTS")

        # check parameters violation
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(0, "DELL", 1, 1, 1)), "ID 0 is illegal")
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(-1, "DELL", 1, 1, 1)), "ID -1 is illegal")
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", 0, 1, 1)), "Speed 0 is illegal")
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", 1, 1, 0)), "Cost 0 is illegal")
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", 1, -5, 1)), "Free space -1 is illegal")
        # check null violation
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(None, "DELL", 1, -1, 1)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(4, None, 10, 10, 10)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", None, 10, 10)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", 10, None, 10)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", 10, 10, None)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(None, None, None, None, None)), "NULL is not allowed")
        # check if disk was added even tho it shouldn't
        disk = Solution.getDiskByID(4)
        self.assertEqual(disk.getDiskID(), None, "badDisk")
        self.assertEqual(disk.getCompany(), None, "badDisk")
        self.assertEqual(disk.getSpeed(), None, "badDisk")
        self.assertEqual(disk.getCost(), None, "badDisk")
        self.assertEqual(disk.getFreeSpace(), None, "badDisk")

        # check error order
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(1, "DELL", 0, 10, 10)),
                         "BAD_PARAMS > ALREADY_EXISTS")
        self.assertEqual(Status.BAD_PARAMS, Solution.addDisk(Disk(1, None, 10, 10, 10)),
                         "BAD_PARAMS > ALREADY_EXISTS")

        # check adding more disks
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(4, "APPLE", 1, 1, 1)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(5, "ASUS", 2, 8, 19)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(6, "DELL", 10, 0, 10)), "Should work")

        # check disk was added and get works
        disk = Solution.getDiskByID(3)
        self.assertEqual(disk.getDiskID(), 3, "Should work")
        self.assertEqual(disk.getCompany(), "DELL", "Should work")
        self.assertEqual(disk.getSpeed(), 10, "Should work")
        self.assertEqual(disk.getCost(), 10, "Should work")
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")

        # check delete twice
        self.assertEqual(Status.OK, Solution.deleteDisk(2), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.deleteDisk(2), "disk 2 was removed")

        # enter a disk that was previously deleted (we add it again after checking)
        self.assertEqual(Status.OK, Solution.addDisk(Disk(62, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteDisk(62), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(62, "DELL", 10, 10, 10)), "should work")
        self.assertEqual(Status.OK, Solution.deleteDisk(62), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(62, "DELL", 10, 10, 10)), "should work")

        # check clear works
        Solution.clearTables()
        self.assertEqual(Status.NOT_EXISTS, Solution.deleteDisk(1), "should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 5, 7, 1)), "Should work")

        # check getting disk that doesn't exist
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getDiskID(), None, "badDisk")
        self.assertEqual(disk.getCompany(), None, "badDisk")
        self.assertEqual(disk.getSpeed(), None, "badDisk")
        self.assertEqual(disk.getCost(), None, "badDisk")
        self.assertEqual(disk.getFreeSpace(), None, "badDisk")

    def test_RAM(self) -> None:
        # check without tables
        Solution.dropTables()
        self.assertEqual(Status.ERROR, Solution.addRAM(RAM(1, "DELL", 1)), "ERROR in case of a database error")
        self.assertEqual(Status.ERROR, Solution.deleteRAM(1), "ERROR in case of a database error")
        Solution.createTables()

        # basic test
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(2, "DELL", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(3, "DELL", 10)), "Should work")
        # check parameters violation
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addRAM(RAM(1, "DELL", 10)),
                         "ID 1 ALREADY_EXISTS")
        self.assertEqual(Status.BAD_PARAMS, Solution.addRAM(RAM(0, "DELL", 10)),
                         "IDs and size are positive (>0) integers")
        self.assertEqual(Status.BAD_PARAMS, Solution.addRAM(RAM(4, "DELL", 0)),
                         "IDs and size are positive (>0) integers")
        self.assertEqual(Status.BAD_PARAMS, Solution.addRAM(RAM(4, "DELL", -1)),
                         "IDs and size are positive (>0) integers")
        # check null violation
        self.assertEqual(Status.BAD_PARAMS, Solution.addRAM(RAM(None, "DELL", 10)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addRAM(RAM(4, None, 10)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addRAM(RAM(4, "DELL", None)), "NULL is not allowed")
        # check if RAM was added anyway
        ram = Solution.getRAMByID(4)
        self.assertEqual(ram.getRamID(), None, "badRAM")
        self.assertEqual(ram.getCompany(), None, "badRAM")
        self.assertEqual(ram.getSize(), None, "badRAM")
        # check error order
        self.assertEqual(Status.BAD_PARAMS, Solution.addRAM(RAM(1, None, 10)),
                         "BAD_PARAMS > ALREADY_EXISTS")
        self.assertEqual(Status.BAD_PARAMS, Solution.addRAM(RAM(1, "DELL", 0)),
                         "BAD_PARAMS > ALREADY_EXISTS")
        # check get func
        ram = Solution.getRAMByID(1)
        self.assertEqual(ram.getRamID(), 1, "Should work")
        self.assertEqual(ram.getCompany(), "DELL", "Should work")
        self.assertEqual(ram.getSize(), 10, "Should work")
        # check delete twice
        self.assertEqual(Status.OK, Solution.addRAM(RAM(4, "DELL", 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addRAM(RAM(4, "DELL", 10)), "RAM 4 already exists")
        self.assertEqual(Status.OK, Solution.deleteRAM(4), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.deleteRAM(4), "ID 4 was already removed")

        # check re-enter with delete in between
        self.assertEqual(Status.OK, Solution.addRAM(RAM(5, "DELL", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteRAM(5), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(5, "DELL", 5)), "Re-adding RAM 1")
        # check clear tables working
        Solution.clearTables()
        self.assertEqual(Status.NOT_EXISTS, Solution.deleteRAM(1), "Should work")
        ram = Solution.getRAMByID(1)
        self.assertEqual(ram.getRamID(), None, "badRAM")
        self.assertEqual(ram.getCompany(), None, "badRAM")
        self.assertEqual(ram.getSize(), None, "badRAM")

    def test_File(self) -> None:
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(3, "CHECK IF NO LIMIT ON CHARS"
                                                             "BY ANY MISTAKE CHARS SHOULD"
                                                             "NOT BE LIMITED TO ANY AMOUNT"
                                                             "OF CHARS", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(4, "MP3", 0)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addFile(File(1, "MP3", 10)), "ID 1 ALREADY_EXISTS")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addFile(File(1, "TXT", 10)), "ID 1 ALREADY_EXISTS")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addFile(File(1, "MP3", 1000)), "ID 1 ALREADY_EXISTS")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addFile(File(1, "MP3", 10)), "SAME DISK ALREADY EXISTS")

        # check parameters violation
        self.assertEqual(Status.BAD_PARAMS, Solution.addFile(File(0, "MP3", 10)), "ID > 0")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFile(File(4, "MP3", -1)), "Size >= 0")
        # check null parameters
        self.assertEqual(Status.BAD_PARAMS, Solution.addFile(File(None, "MP3", 10)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFile(File(4, None, 10)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFile(File(4, "MP3", None)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFile(File(4, None, None)), "NULL is not allowed")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFile(File(None, None, None)), "NULL is not allowed")
        # check errors order
        self.assertEqual(Status.BAD_PARAMS, Solution.addFile(File(1, "MP3", -1)),
                         "BAD_PARAMS before ALREADY_EXISTS")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFile(File(1, None, 0)),
                         "BAD_PARAMS before ALREADY_EXISTS")
        # check get func
        file = Solution.getFileByID(1)
        self.assertEqual(file.getFileID(), 1, "Should work")
        self.assertEqual(file.getType(), "MP3", "Should work")
        self.assertEqual(file.getSize(), 10, "Should work")
        # check delete
        self.assertEqual(Status.OK, Solution.deleteFile(File(4, "MP3", 0)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(4, "MP3", 0)), "Should work")
        file = Solution.getFileByID(4)
        self.assertEqual(file.getFileID(), None, "file should be deleted")
        self.assertEqual(file.getType(), None, "file should be deleted")
        self.assertEqual(file.getSize(), None, "file should be deleted")
        # check re-enter with delete in between
        self.assertEqual(Status.OK, Solution.addFile(File(23, "MP3", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(23, "MP3", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(23, "MP3", 5)), "Re-adding RAM 1")
        # check without tables
        Solution.dropTables()
        self.assertEqual(Status.ERROR, Solution.addFile(File(1, "MP3", 1)), "Should error")
        self.assertEqual(Status.ERROR, Solution.deleteFile(File(1, "MP3", 1)), "Should error")
        # should be empty
        file = Solution.getFileByID(1)
        self.assertEqual(file.getFileID(), None, "badFile")
        self.assertEqual(file.getType(), None, "badFile")
        self.assertEqual(file.getSize(), None, "badFile")

    def test_addDiskAndFile(self) -> None:
        # check without tables
        Solution.dropTables()
        self.assertEqual(Status.ERROR, Solution.addDiskAndFile(Disk(1, "DELL", 10, 10, 10),
                                                               File(1, "MP3", 0)),
                         "ERROR in case of a database error")
        self.assertEqual(Status.ERROR, Solution.deleteDisk(1), "ERROR in case of a database error")
        self.assertEqual(Status.ERROR, Solution.deleteFile(File(1, "MP3", 0)), "ERROR in case of a database error")
        Solution.createTables()
        # basic test
        self.assertEqual(Status.OK, Solution.addDiskAndFile(Disk(1, "DELL", 10, 10, 10),
                                                            File(1, "MP3", 0)), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getDiskID(), 1, "Should work")
        self.assertEqual(disk.getCompany(), "DELL", "Should work")
        self.assertEqual(disk.getSpeed(), 10, "Should work")
        self.assertEqual(disk.getCost(), 10, "Should work")
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        file = Solution.getFileByID(1)
        self.assertEqual(file.getFileID(), 1, "Should work")
        self.assertEqual(file.getType(), "MP3", "Should work")
        self.assertEqual(file.getSize(), 0, "Should work")
        # check if only 1 of the 2 is being added
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addDiskAndFile(Disk(1, "DELL", 10, 10, 10),
                                                                        File(2, "MP3", 0)), "Should work")
        file = Solution.getFileByID(2)
        self.assertEqual(file.getFileID(), None, "shouldn't been added")
        self.assertEqual(file.getFileID(), None, "shouldn't been added")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 0)),
                         "check if file 2 was added even tho disk exists")
        file = Solution.getFileByID(2)
        self.assertEqual(file.getFileID(), 2, "Should work")
        self.assertEqual(file.getType(), "MP3", "Should work")
        self.assertEqual(file.getSize(), 0, "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addDiskAndFile(Disk(2, "DELL", 10, 10, 10),
                                                                        File(2, "MP3", 0)), "Should work")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getDiskID(), None, "shouldn't been added")
        self.assertEqual(Status.NOT_EXISTS, Solution.deleteDisk(2), "shouldn't been added")

    def test_add_and_remove_RAM_from_disk(self):
        # setup
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        # check errors
        self.assertEqual(Status.NOT_EXISTS, Solution.addRAMToDisk(1, 4), "Disk doesn't exist")
        self.assertEqual(Status.NOT_EXISTS, Solution.addRAMToDisk(8, 1), "RAM doesn't exist")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addRAMToDisk(1, 1), "RAM already on disk")
        self.assertEqual(Status.NOT_EXISTS, Solution.removeRAMFromDisk(1, 4), "Disk doesn't exist")
        self.assertEqual(Status.NOT_EXISTS, Solution.removeRAMFromDisk(16, 1), "RAM doesn't exist")
        # check re-enter to verify removal
        self.assertEqual(Status.OK, Solution.removeRAMFromDisk(1, 1), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.removeRAMFromDisk(1, 1), "RAM already removed")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        # check that deleting ram removes it from disk
        self.assertEqual(Status.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.removeRAMFromDisk(1, 1),
                         "RAM should have been removed when deleted")
        # check with more disks/ram
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(2, "DELL", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 2), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 2), "Should work")
        # check if deleting ram removes it from all disks
        self.assertEqual(Status.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.removeRAMFromDisk(1, 2),
                         "RAM should've been removed")
        self.assertEqual(Status.NOT_EXISTS, Solution.removeRAMFromDisk(1, 1),
                         "RAM should've been removed")
        # check if ram 2 wasn't removed
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addRAMToDisk(2, 1), "RAM is already on disk")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addRAMToDisk(2, 2), "RAM is already on disk")
        # check if removing disk doesn't mes-up with other disks ram
        self.assertEqual(Status.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.addRAMToDisk(2, 1), "Disk was deleted")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addRAMToDisk(2, 2), "RAM is already on disk")
        # check without tables
        Solution.dropTables()
        self.assertEqual(Status.ERROR, Solution.addRAMToDisk(1, 1), "Should error")
        self.assertEqual(Status.ERROR, Solution.removeRAMFromDisk(1, 1), "Should error")

    def test_add_and_remove_file_from_disk(self):
        self.assertEqual(Status.OK, Solution.addDiskAndFile(Disk(1, "DELL", 10, 10, 10),
                                                            File(1, "MP3", 5)), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        # check errors
        self.assertEqual(Status.NOT_EXISTS, Solution.addFileToDisk(File(5, "MP3", 0), 1), "File does not exist")
        self.assertEqual(Status.NOT_EXISTS, Solution.addFileToDisk(File(1, "MP3", 7), 5), "Disk does not exist")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addFileToDisk(File(1, "MP3", 7), 1),
                         "ALREADY_EXISTS before BAD_PARAMS")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 6)), "Should work")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFileToDisk(File(2, "MP3", 6), 1), "No space")
        # check fill disk to 0
        self.assertEqual(Status.OK, Solution.addFile(File(3, "MP3", 5)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(3, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 0, "Should work")
        # check remove func errors
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(4, "MP3", 10), 1), "No File "
                                                                                        "should still return OK")
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(1, "MP3", 5), 2), "No Disk "
                                                                                       "should still return OK")
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(2, "MP3", 6), 1), "File not on Disk"
                                                                                       "should still return OK")
        # check if remove func works
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(1, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(3, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(1, "MP3", 5), 1), "File not on Disk "
                                                                                       "should still return OK")
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(3, "MP3", 5), 1), "File not on Disk "
                                                                                       "should still return OK")
        # check if free space haven't changed
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        # check if deleting file which is on one disk effects other disks
        Solution.clearTables()
        self.assertEqual(Status.OK, Solution.addDiskAndFile(Disk(1, "DELL", 10, 10, 10),
                                                            File(1, "MP3", 5)), "Should work")
        self.assertEqual(Status.OK, Solution.addDiskAndFile(Disk(2, "DELL", 20, 20, 20),
                                                            File(2, "MP3", 5)), "Should work")
        # sanity check
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 20, "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 20, "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(2, "MP3", 5)), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "File was deleted")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 20, "shouldn't been effected by delete")
        # check if file was deleted and not just removed from disks
        self.assertEqual(Status.NOT_EXISTS, Solution.addFileToDisk(File(2, "MP3", 5), 1), "File doesn't exist")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "shouldn't been effected by delete")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 20, "shouldn't been effected by delete")
        # check if removing from one disk doesn't affect other disks
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 5), 1), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 5), 2), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 15, "Should work")
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(1, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 15, "shouldn't been effected")
        # check if deleting after removing works fine
        self.assertEqual(Status.OK, Solution.deleteFile(File(1, "MP3", 5)), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "shouldn't been effected")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 20, "Should work")
        # check if deleting disk effects on files on disks
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 5)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 5), 1), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 5), 2), "Should work")
        self.assertEqual(Status.OK, Solution.deleteDisk(1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), None, "Disk deleted")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 15, "Should work")
        # check re-entering same file and disk ids  #FAILURE on line 394-406
        Solution.clearTables()
        Solution.createTables()
        self.assertEqual(Status.OK, Solution.addDiskAndFile(Disk(1, "DELL", 10, 10, 10),
                                                            File(1, "MP3", 5)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 5), 1), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(1, "MP3", 5)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 11)), "Should work")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFileToDisk(File(1, "MP3", 11), 1), "File too big now")
        self.assertEqual(Status.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 20, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 11), 1), "Should work")
        # check for errors of database
        Solution.dropTables()
        self.assertEqual(Status.ERROR, Solution.addFileToDisk(File(1, "MP3", 6), 1), "Should error")
        self.assertEqual(Status.ERROR, Solution.removeFileFromDisk(File(1, "MP3", 6), 1), "Should error")

    def test_averageFileSizeOnDisk(self):
        # base cases of ID doesn't exist and no files and database errors
        Solution.dropTables()
        self.assertEqual(-1, Solution.averageFileSizeOnDisk(1), "-1 in case of other errors")
        Solution.createTables()
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        # check if added properly
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 2)), "Should work")
        # check if added file doesn't change average size
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "File wasn't added yet")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 2), 1), "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(5), "0 in case of division by 0 or if ID does not exist")
        # check if file was added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 8, "Should work")
        self.assertEqual(2, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 3)), "Should work")
        self.assertEqual(2, Solution.averageFileSizeOnDisk(1), "Shouldn't change")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 3), 1), "Should work")
        # check if file was added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(2.5, Solution.averageFileSizeOnDisk(1), "Should work")
        # check if files which shouldn't get added effect average size
        self.assertEqual(Status.OK, Solution.addFile(File(8, "MP3", 8)), "Should work")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFileToDisk(File(8, "MP3", 8), 1), "No space")
        self.assertEqual(2.5, Solution.averageFileSizeOnDisk(1), "Should work")
        # check if file was added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(2.5, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addFileToDisk(File(2, "MP3", 3), 1), "Already on disk")
        # check if file was added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.addFileToDisk(File(3, "MP3", 2), 1), "Doesn't exist")
        # check if file was added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(2.5, Solution.averageFileSizeOnDisk(1), "Should work")
        # check remove/delete file effect on average size
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(1, "MP3", 2), 1), "Should work")
        # check if file was removed
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 7, "Should work")
        self.assertEqual(3, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(2, "MP3", 3)), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(8, "MP3", 8), 1), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 2), 1), "Should work")
        # check if files were added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 0, "Should work")
        self.assertEqual(5, Solution.averageFileSizeOnDisk(1), "Should work")
        # check disk deletion works fine
        self.assertEqual(Status.OK, Solution.deleteDisk(1), "Should work")
        # check if disk was deleted
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getDiskID(), None, "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        # check if adding file to non-existing disk doesn't create the disk
        self.assertEqual(Status.NOT_EXISTS, Solution.addFileToDisk(File(1, "MP3", 3), 1), "Disk deleted")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        # check if division by 0 returns 0
        Solution.clearTables()
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 0)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 0), 1), "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 0)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 0), 1), "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        self.assertEqual(Status.OK, Solution.addFile(File(3, "MP3", 0)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(3, "MP3", 0), 1), "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        # check with 2 disks
        Solution.clearTables()
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 1)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 2)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 1), 1), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 2), 1), "Should work")
        self.assertEqual(1.5, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(2), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 3), 2), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 5), 2), "Should work")
        self.assertEqual(1.5, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(1.5, Solution.averageFileSizeOnDisk(2), "Should work")
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(1, "MP3", 1), 2), "Should work")
        self.assertEqual(1.5, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(2, Solution.averageFileSizeOnDisk(2), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(2, "MP3", 2)), "Should work")
        self.assertEqual(1, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(2), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 1), 2), "Should work")
        self.assertEqual(Status.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(1, Solution.averageFileSizeOnDisk(2), "Should work")
        # test with several files and disks
        Solution.clearTables()
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 20, 20, 20)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(3, "DELL", 30, 30, 30)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 1)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 2)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(5, "MP3", 5)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(6, "MP3", 6)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(10, "MP3", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 1), 3), "Should work")
        self.assertEqual(1, Solution.averageFileSizeOnDisk(3), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(5, "MP3", 5), 3), "Should work")
        self.assertEqual(3, Solution.averageFileSizeOnDisk(3), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(6, "MP3", 6), 3), "Should work")
        self.assertEqual(4, Solution.averageFileSizeOnDisk(3), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 2), 3), "Should work")
        self.assertEqual(3.5, Solution.averageFileSizeOnDisk(3), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(10, "MP3", 10), 3), "Should work")
        self.assertEqual(4.8, Solution.averageFileSizeOnDisk(3), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(10, "MP3", 10), 1), "Should work")
        self.assertEqual(10, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(10, "MP3", 10)), "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(1), "Should work")
        self.assertEqual(0, Solution.averageFileSizeOnDisk(2), "Should work")
        self.assertEqual(3.5, Solution.averageFileSizeOnDisk(3), "Should work")

    def test_diskTotalRAM(self):
        # basic errors
        Solution.dropTables()
        self.assertEqual(-1, Solution.diskTotalRAM(1), "-1 in case of other errors")
        Solution.createTables()
        self.assertEqual(0, Solution.diskTotalRAM(1), "0 if the disk does not exist")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.addRAMToDisk(1, 1), "RAM wasn't added")
        self.assertEqual(0, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "DELL", 1)), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(1), "Should work")
        # check add ram func is working
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(1, Solution.diskTotalRAM(1), "Should work")
        # check removing non-existing ram does anything
        self.assertEqual(Status.NOT_EXISTS, Solution.removeRAMFromDisk(2, 1), "RAM doesn't exist")
        self.assertEqual(1, Solution.diskTotalRAM(1), "Should work")
        # check if non-added ram can't be removed
        self.assertEqual(Status.OK, Solution.addRAM(RAM(2, "DELL", 1)), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.removeRAMFromDisk(2, 1), "We haven't added RAM 2 to disk yet")
        self.assertEqual(1, Solution.diskTotalRAM(1), "Should work")
        # check re-adding RAM
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addRAMToDisk(1, 1), "already added")
        self.assertEqual(1, Solution.diskTotalRAM(1), "Should work")
        # check with more than 1 RAM
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(2, Solution.diskTotalRAM(1), "Should work")
        # check if removing/deleting RAM works properly
        self.assertEqual(Status.OK, Solution.removeRAMFromDisk(1, 1), "Should work")
        self.assertEqual(1, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(Status.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(1, Solution.diskTotalRAM(1), "Shouldn't change")
        self.assertEqual(Status.OK, Solution.deleteRAM(2), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "DELL", 1)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(1, Solution.diskTotalRAM(1), "Should work")
        # check if deleting disk with RAM works
        self.assertEqual(Status.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(1), "0 if the disk does not exist")
        # check with more than 1 disk
        Solution.clearTables()
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "DELL", 1)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(2, "DELL", 2)), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(2), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(1, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(2), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(3, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(2), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 2), "Should work")
        self.assertEqual(3, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(1, Solution.diskTotalRAM(2), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 2), "Should work")
        self.assertEqual(3, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(3, Solution.diskTotalRAM(2), "Should work")
        self.assertEqual(Status.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(2, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(2, Solution.diskTotalRAM(2), "Should work")
        # check re-entering RAM after deleting it
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "DELL", 1)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(3, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(2, Solution.diskTotalRAM(2), "Should work")
        self.assertEqual(Status.OK, Solution.removeRAMFromDisk(2, 2), "Should work")
        self.assertEqual(3, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(2), "Should work")
        # check if deleting ram doesn't update disks it shouldn't
        self.assertEqual(Status.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(2, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(2), "Should work")
        # check deleting disk works properly with RAM
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 2), "Should work")
        self.assertEqual(2, Solution.diskTotalRAM(1), "Should work")
        self.assertEqual(2, Solution.diskTotalRAM(2), "Should work")
        self.assertEqual(Status.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(1), "0 if the disk does not exist")
        self.assertEqual(2, Solution.diskTotalRAM(2), "Should work")
        self.assertEqual(Status.OK, Solution.deleteRAM(2), "Should work")
        self.assertEqual(0, Solution.diskTotalRAM(2), "Should work")

    def test_getCostForType(self):
        # check database error
        Solution.dropTables()
        self.assertEqual(-1, Solution.getCostForType("MP3"), "-1 in case of other errors")
        # doesn't exist error
        Solution.createTables()
        self.assertEqual(0, Solution.getCostForType("WAV"), "0 if the type does not exist")
        # check disk without files on it
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 2)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 50, 10)), "Should work")
        self.assertEqual(0, Solution.getCostForType("MP3"), "Should work")
        # check basic functionality
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 5), 1), "Should work")
        self.assertEqual(20, Solution.getCostForType("MP3"), "Should work")
        # check if file doesn't get added to count after error
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addFileToDisk(File(1, "MP3", 2), 1), "Already added")
        self.assertEqual(20, Solution.getCostForType("MP3"), "Shouldn't change")
        self.assertEqual(Status.OK, Solution.addFile(File(10, "MP3", 100)), "Should work")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFileToDisk(File(10, "MP3", 100), 1), "No space")
        self.assertEqual(20, Solution.getCostForType("MP3"), "Shouldn't change")
        self.assertEqual(Status.OK, Solution.deleteFile(File(10, "MP3", 100)), "Should work")
        self.assertEqual(20, Solution.getCostForType("MP3"), "Shouldn't change")
        # check case of disk with file on it and files which aren't on the disk
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 3)), "Should work")
        self.assertEqual(20, Solution.getCostForType("MP3"), "File 2 wasn't added yet")
        # check with more than 1 disk
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 50, 20)), "Should work")
        self.assertEqual(20, Solution.getCostForType("MP3"), "Shouldn't change")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 3), 1), "Should work")
        self.assertEqual(50, Solution.getCostForType("MP3"), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(3, "MP3", 4)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(3, "MP3", 4), 2), "Should work")
        self.assertEqual(130, Solution.getCostForType("MP3"), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 3), 2), "Should work")
        self.assertEqual(190, Solution.getCostForType("MP3"), "Should work")
        # check with more than 1 file type
        self.assertEqual(Status.OK, Solution.addFile(File(4, "MP4", 5)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(5, "MP4", 6)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(4, "MP4", 5), 1), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(5, "MP4", 6), 2), "Should work")
        self.assertEqual(190, Solution.getCostForType("MP3"), "MP4 != MP3")
        self.assertEqual(170, Solution.getCostForType("MP4"), "MP4 != MP3")
        # check if deleting files works properly
        self.assertEqual(Status.OK, Solution.deleteFile(File(4, "MP4", 5)), "Should work")
        self.assertEqual(190, Solution.getCostForType("MP3"), "Shouldn't changed")
        self.assertEqual(120, Solution.getCostForType("MP4"), "Should work")
        # check if deleting disks works properly
        self.assertEqual(Status.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(140, Solution.getCostForType("MP3"), "should calculate only from disk 2")
        self.assertEqual(120, Solution.getCostForType("MP4"), "Should work")

    def test_getFilesCanBeAddedToDisk(self):
        # check database error
        Solution.dropTables()
        self.assertEqual([], Solution.getFilesCanBeAddedToDisk(1), "Empty List in any other case")
        # check disk doesn't exist error
        Solution.createTables()
        self.assertListEqual([], Solution.getFilesCanBeAddedToDisk(1), "Empty List in any other case")
        # check without files
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertListEqual([], Solution.getFilesCanBeAddedToDisk(1), "Should work")
        # basic functionality
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 1)), "Should work")
        self.assertListEqual([1], Solution.getFilesCanBeAddedToDisk(1), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 2)), "Should work")
        self.assertListEqual([2, 1], Solution.getFilesCanBeAddedToDisk(1), "IDs in descending order")
        # check with more disks
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 20, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(3, "MP3", 3)), "Should work")
        self.assertListEqual([3, 2, 1], Solution.getFilesCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([3, 2, 1], Solution.getFilesCanBeAddedToDisk(2), "Should work")
        # check with file that can't fit into one disk
        self.assertEqual(Status.OK, Solution.addFile(File(4, "MP3", 11)), "Should work")
        self.assertListEqual([3, 2, 1], Solution.getFilesCanBeAddedToDisk(1), "11 > 10")
        self.assertListEqual([4, 3, 2, 1], Solution.getFilesCanBeAddedToDisk(2), "11 < 20")
        # check with file that can't fit into any disk
        self.assertEqual(Status.OK, Solution.addFile(File(5, "MP3", 50)), "Should work")
        self.assertListEqual([3, 2, 1], Solution.getFilesCanBeAddedToDisk(1), "Disk 1 shouldn't have space")
        self.assertListEqual([4, 3, 2, 1], Solution.getFilesCanBeAddedToDisk(2), "Disk 2 shouldn't have space")
        # check if file name doesn't affect order
        self.assertEqual(Status.OK, Solution.addFile(File(6, "Z", 1)), "Should work")
        self.assertListEqual([6, 3, 2, 1], Solution.getFilesCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([6, 4, 3, 2, 1], Solution.getFilesCanBeAddedToDisk(2), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(9, "A", 1)), "Should work")
        self.assertListEqual([9, 6, 3, 2, 1], Solution.getFilesCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([9, 6, 4, 3, 2], Solution.getFilesCanBeAddedToDisk(2), "Should work")
        # check if delete works properly
        self.assertEqual(Status.OK, Solution.deleteFile(File(4, "MP3", 11)), "Should work")
        self.assertListEqual([9, 6, 3, 2, 1], Solution.getFilesCanBeAddedToDisk(1), "Shouldn't change")
        self.assertListEqual([9, 6, 3, 2, 1], Solution.getFilesCanBeAddedToDisk(2), "file 1 should be here"
                                                                                    " instead of file 4")
        self.assertEqual(Status.OK, Solution.deleteFile(File(3, "MP3", 3)), "Should work")
        self.assertListEqual([9, 6, 2, 1], Solution.getFilesCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([9, 6, 2, 1], Solution.getFilesCanBeAddedToDisk(2), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(10, "MP3", 5)), "Should work")
        self.assertListEqual([10, 9, 6, 2, 1], Solution.getFilesCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([10, 9, 6, 2, 1], Solution.getFilesCanBeAddedToDisk(2), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(11, "MP3", 6)), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getFilesCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getFilesCanBeAddedToDisk(2), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(1, "MP3", 1)), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getFilesCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getFilesCanBeAddedToDisk(2), "Should work")
        # check if adding file to disk works properly
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(11, "MP3", 6), 1), "Should work")
        self.assertListEqual([9, 6, 2], Solution.getFilesCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getFilesCanBeAddedToDisk(2), "Shouldn't change")
        # check if file which is already on disk is counted
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(10, "MP3", 5), 2), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getFilesCanBeAddedToDisk(2), "Shouldn't change")
        # check if deleting disk works properly
        self.assertEqual(Status.OK, Solution.deleteDisk(1), "Should work")
        self.assertListEqual([], Solution.getFilesCanBeAddedToDisk(1), "Empty List in any other case")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getFilesCanBeAddedToDisk(2), "Shouldn't change")

    def test_getFilesCanBeAddedToDiskAndRAM(self):
        # check database error
        Solution.dropTables()
        self.assertEqual([], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Empty List in any other case")
        # check no disk error
        Solution.createTables()
        self.assertListEqual([], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Empty List in any other case")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        # check without files and ram on disk
        self.assertListEqual([], Solution.getFilesCanBeAddedToDiskAndRAM(1), "No files")
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 1)), "Should work")
        self.assertListEqual([], Solution.getFilesCanBeAddedToDiskAndRAM(1), "No RAM yet")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        self.assertListEqual([], Solution.getFilesCanBeAddedToDiskAndRAM(1), "RAM wasn't added yet")
        # basic test
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertListEqual([1], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Should work")
        # check with more than 1 disk
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 20, 10)), "Should work")
        self.assertListEqual([], Solution.getFilesCanBeAddedToDiskAndRAM(2), "No RAM on disk 2")
        # check with more RAM
        self.assertEqual(Status.OK, Solution.addRAM(RAM(2, "DELL", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertListEqual([1], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Shouldn't change")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 2), "Should work")
        self.assertListEqual([1], Solution.getFilesCanBeAddedToDiskAndRAM(2), "Should work")
        # check case where there's room on RAM or on disk
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 19)), "Should work")
        self.assertListEqual([1], Solution.getFilesCanBeAddedToDiskAndRAM(2), "RAM space")
        self.assertListEqual([1], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Disk space")
        # check functionality of removing RAM
        self.assertEqual(Status.OK, Solution.removeRAMFromDisk(2, 2), "Should work")
        self.assertListEqual([], Solution.getFilesCanBeAddedToDiskAndRAM(2), "Should work")
        self.assertListEqual([1], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Should work")
        # check functionality of deleting RAM
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 2), "Should work")
        self.assertEqual(Status.OK, Solution.deleteRAM(2), "Should work")
        self.assertListEqual([], Solution.getFilesCanBeAddedToDiskAndRAM(2), "Disk 2 has no RAM")
        self.assertListEqual([1], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Should work")
        # check if deleting disk doesn't remove RAM from other disks
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 2), "Should work")
        self.assertEqual(Status.OK, Solution.deleteDisk(2), "Should work")
        self.assertListEqual([1], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Should work")
        self.assertListEqual([], Solution.getFilesCanBeAddedToDiskAndRAM(2), "Empty List in any other case")
        # check order is correct and func with more files on system
        self.assertEqual(Status.OK, Solution.addFile(File(4, "MP3", 2)), "Should work")
        self.assertListEqual([1, 4], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Ascending order")
        self.assertEqual(Status.OK, Solution.addFile(File(3, "MP3", 1)), "Should work")
        self.assertListEqual([1, 3, 4], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Ascending order")
        # check if no more than 5 are returned
        self.assertEqual(Status.OK, Solution.addFile(File(7, "MP3", 1)), "Should work")
        self.assertListEqual([1, 3, 4, 7], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(10, "MP3", 1)), "Should work")
        self.assertListEqual([1, 3, 4, 7, 10], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(11, "MP3", 1)), "Should work")
        self.assertListEqual([1, 3, 4, 7, 10], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Limit 5")
        # check if deleting file works properly
        self.assertEqual(Status.OK, Solution.deleteFile(File(10, "MP3", 1)), "Should work")
        self.assertListEqual([1, 3, 4, 7, 11], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Should work")
        # check if adding File to Disk keeps the file included in the list
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(7, "MP3", 1), 1), "Should work")
        self.assertListEqual([1, 3, 4, 7, 11], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Should work")
        # check if deleting ram works properly with files on disk and files in system
        self.assertEqual(Status.OK, Solution.deleteRAM(1), "Should work")
        self.assertListEqual([], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Should be empty, no RAM")
        # check if file of size 0 works
        self.assertEqual(Status.OK, Solution.addFile(File(100, "TXT", 0)), "Should work")
        self.assertListEqual([100], Solution.getFilesCanBeAddedToDiskAndRAM(1), "Should work")

    def test_isCompanyExclusive(self):
        # check database error
        Solution.dropTables()
        self.assertEqual(False, Solution.isCompanyExclusive(1), "False in case of an error "
                                                                "or the disk does not exist")
        Solution.createTables()
        # check for error of disk doesn't exist
        self.assertEqual(False, Solution.isCompanyExclusive(1), "False in case of an error "
                                                                "or the disk does not exist")
        # setup
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        # true for no RAM on disk
        self.assertEqual(True, Solution.isCompanyExclusive(1), "Should work")
        # check with unconnected RAM
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(2, "Lenovo", 10)), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "Shouldn't change")
        # basic test
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "Should work")
        self.assertEqual(Status.OK, Solution.removeRAMFromDisk(1, 1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "DELL was removed, Lenovo still on disk 1")
        self.assertEqual(Status.OK, Solution.removeRAMFromDisk(2, 1), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "No RAM on disk 1")
        # check if deleting ram while it's on disk works properly
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(Status.OK, Solution.deleteRAM(1), "should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "no RAM on disk 1")
        # check with more than 1 exclusive RAM
        self.assertEqual(Status.OK, Solution.deleteRAM(2), "should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(2, "DELL", 5)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "Should work")
        # check if exclusivity is broken with 2 proper RAMs and 1 non-exclusive
        self.assertEqual(Status.OK, Solution.addRAM(RAM(3, "APPLE", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(3, 1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "Should work")
        # check with more than one disk
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "APPLE", 5, 5, 5)), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(2), "Should work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(3, 2), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(2), "Shou;d work")
        self.assertEqual(Status.OK, Solution.addRAMToDisk(2, 2), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "DELL != APPLE")
        # check if deleting non-existing RAM effects results
        self.assertEqual(Status.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "Shouldn't change")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "Shouldn't change")
        # check if deleting existing RAM works properly
        self.assertEqual(Status.OK, Solution.deleteRAM(3), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(2), "Only Dell RAM on APPLE disk")
        # check if deleting disk effects other disks
        self.assertEqual(Status.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "Disk was deleted")
        self.assertEqual(False, Solution.isCompanyExclusive(2), "Shouldn't change")

    def test_getConflictingDisks(self):
        # check database error
        Solution.dropTables()
        self.assertListEqual([], Solution.getConflictingDisks(), "Empty List in any other case")
        Solution.createTables()
        # check empty database
        self.assertListEqual([], Solution.getConflictingDisks(), "Should work")
        # check with non-conflicting disks
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertListEqual([], Solution.getConflictingDisks(), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertListEqual([], Solution.getConflictingDisks(), "Should work")
        # check with file on system but not on disk
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 2)), "Should work")
        self.assertListEqual([], Solution.getConflictingDisks(), "Should work")
        # check with conflicting files
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 2), 1), "Should work")
        self.assertListEqual([], Solution.getConflictingDisks(), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 2), 2), "Should work")
        self.assertListEqual([1, 2], Solution.getConflictingDisks(), "File 1 runs on both disks")
        # try with more disks
        self.assertEqual(Status.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertListEqual([1, 2], Solution.getConflictingDisks(), "Shouldn't change")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 2), 3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getConflictingDisks(), "File 1 on all disks")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(4, "DELL", 1, 1, 1)), "Should work")
        # check with disk without enough space for file
        self.assertListEqual([1, 2, 3], Solution.getConflictingDisks(), "Shouldn't change")
        self.assertEqual(Status.BAD_PARAMS, Solution.addFileToDisk(File(1, "MP3", 2), 4), "No space")
        self.assertListEqual([1, 2, 3], Solution.getConflictingDisks(), "Shouldn't change")
        # check with more files
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 1)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 1), 4), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getConflictingDisks(), "Shouldn't change yet")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 1), 1), "Should work")
        self.assertListEqual([1, 2, 3, 4], Solution.getConflictingDisks(), "4 conflicting with 1")
        # check if removing file works properly
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(1, "MP3", 2), 2), "Should work")
        self.assertListEqual([1, 3, 4], Solution.getConflictingDisks(), "Should change")
        # check if deleting files works properly
        self.assertEqual(Status.OK, Solution.deleteFile(File(1, "MP3", 2)), "Should work")
        self.assertListEqual([1, 4], Solution.getConflictingDisks(), "Only file 2 on 2 disks")
        # check if deleting disk doesn't mess system
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 1), 2), "Should work")
        self.assertListEqual([1, 2, 4], Solution.getConflictingDisks(), "Should work")
        self.assertEqual(Status.OK, Solution.deleteDisk(2), "Should work")
        self.assertListEqual([1, 4], Solution.getConflictingDisks(), "Should work")

    def test_mostAvailableDisks(self):
        # check database error
        Solution.dropTables()
        self.assertListEqual([], Solution.mostAvailableDisks(), "Empty List in any other case")
        Solution.createTables()
        # check without disks and files
        self.assertListEqual([], Solution.mostAvailableDisks(), "No disks")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        # 0 files
        self.assertListEqual([1], Solution.mostAvailableDisks(), "Should work")
        # check order is correct
        self.assertEqual(Status.OK, Solution.addDisk(Disk(4, "DELL", 4, 4, 4)), "Should work")
        self.assertListEqual([1, 4], Solution.mostAvailableDisks(), "Disk 1 faster than Disk 2")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertListEqual([1, 2, 4], Solution.mostAvailableDisks(), "disks 1+2 faster than 4"
                                                                       "id 1 < 2")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(3, "DELL", 20, 20, 10)), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Disk 2 fastest")
        # check with files
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 15)), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Only disk 3 has space for it")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 15), 3), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "File 1 can't fit disk 3 twice")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 1)), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Shouldn't change")
        # check if remove file works properly, + disk 3 now can run 2 files
        self.assertEqual(Status.OK, Solution.removeFileFromDisk(File(1, "MP3", 15), 3), "should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(1, "MP3", 15)), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Order only by ID and speed")
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 11)), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Should work")
        # reset files
        Solution.clearTables()
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 5, 5, 5)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 6, 2, 6)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(3, "DELL", 4, 5, 4)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(4, "DELL", 10, 5, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(5, "DELL", 5, 10, 5)), "Should work")
        self.assertListEqual([4, 2, 1, 5, 3], Solution.mostAvailableDisks(), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 3)), "Should work")
        self.assertListEqual([4, 1, 5, 3, 2], Solution.mostAvailableDisks(), "Disk 2 no space")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 3), 1), "Should work")
        self.assertListEqual([4, 5, 3, 2, 1], Solution.mostAvailableDisks(), "Disk 1 can't run file 1 "
                                                                             "twice, and it's slower than disk 2")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 3), 5), "Should work")
        self.assertListEqual([4, 5, 3, 2, 1], Solution.mostAvailableDisks(), "Shouldn't change")
        self.assertEqual(Status.NOT_EXISTS, Solution.addFileToDisk(File(10, "TROJAN", 0), 2), "Shouldn't work")
        self.assertListEqual([4, 5, 3, 2, 1], Solution.mostAvailableDisks(), "Shouldn't change")
        # check limit of 5
        self.assertEqual(Status.OK, Solution.addDisk(Disk(6, "DELL", 1, 1, 1)), "Should work")
        self.assertListEqual([4, 5, 3, 2, 1], Solution.mostAvailableDisks(), "Shouldn't change")

    def test_getCloseFiles(self):
        # database error 
        Solution.dropTables()
        self.assertListEqual([], Solution.getCloseFiles(1), "Empty List in any other case")
        Solution.createTables()
        # check if file isn't close to itself
        self.assertEqual(Status.OK, Solution.addFile(File(1, "MP3", 1)), "Should work")
        self.assertListEqual([], Solution.getCloseFiles(1), "Should work")
        # Note: files can be close in an empty way (file in question isn’t saved on any disk)
        self.assertEqual(Status.OK, Solution.addFile(File(2, "MP3", 2)), "Should work")
        self.assertListEqual([1], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([2], Solution.getCloseFiles(1), "Should work")
        self.assertListEqual([2], Solution.getCloseFiles(1), "Should work the same as before")
        # check with more Files
        self.assertEqual(Status.OK, Solution.addFile(File(3, "MP3", 3)), "Should work")
        self.assertListEqual([1, 2], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([1, 3], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([2, 3], Solution.getCloseFiles(1), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(4, "MP3", 4)), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Should work")
        self.assertListEqual([1, 2, 4], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([1, 3, 4], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([2, 3, 4], Solution.getCloseFiles(1), "Should work")
        # check if list limited to 10 files
        self.assertEqual(Status.OK, Solution.addFile(File(5, "MP3", 5)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(6, "MP3", 6)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(7, "MP3", 7)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(8, "MP3", 8)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(9, "MP3", 9)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(10, "MP3", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(11, "MP3", 11)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(12, "MP3", 12)), "Should work")
        self.assertListEqual([2, 3, 4, 5, 6, 7, 8, 9, 10, 11], Solution.getCloseFiles(1), "Should work")
        # reset stats
        self.assertEqual(Status.OK, Solution.deleteFile(File(5, "MP3", 5)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(6, "MP3", 6)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(7, "MP3", 7)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(8, "MP3", 8)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(9, "MP3", 9)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(10, "MP3", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(11, "MP3", 11)), "Should work")
        self.assertEqual(Status.OK, Solution.deleteFile(File(12, "MP3", 12)), "Should work")
        # check with disk in the system
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        # now:
        #   File 1 on: disks None
        #   File 2 on: disks None
        #   File 3 on: disks None
        #   File 4 on: disks None
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Should work")
        self.assertListEqual([1, 2, 4], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([1, 3, 4], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([2, 3, 4], Solution.getCloseFiles(1), "Should work")
        # check if close in empty set gets cancelled when adding file to disk
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 1), 1), "Should work")
        # now:
        #   File 1 on: disks None
        #   File 2 on: disks None
        #   File 3 on: disks None
        #   File 4 on: disks None
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Should work")
        self.assertListEqual([1, 2, 4], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([1, 3, 4], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([], Solution.getCloseFiles(1), "Should work")
        # check when two files are on same disk
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 2), 1), "Should work")
        # now:
        #   File 1 on: disks 1
        #   File 2 on: disks None
        #   File 3 on: disks None
        #   File 4 on: disks None
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Should work")
        self.assertListEqual([1, 2, 4], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([1], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([2], Solution.getCloseFiles(1), "Should work")
        # check with more disks
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        # now:
        #   File 1 on: disks 1
        #   File 2 on: disks 1
        #   File 3 on: disks None
        #   File 4 on: disks None
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Shouldn't change")
        self.assertListEqual([1, 2, 4], Solution.getCloseFiles(3), "Shouldn't change")
        self.assertListEqual([1], Solution.getCloseFiles(2), "Shouldn't change")
        self.assertListEqual([2], Solution.getCloseFiles(1), "Shouldn't change")
        # check with files on more than one disk
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(3, "MP3", 3), 2), "Should work")
        # now:
        #   File 1 on: disks 1
        #   File 2 on: disks 1
        #   File 3 on: disks 2
        #   File 4 on: disks None
        self.assertListEqual([1], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([2], Solution.getCloseFiles(1), "Should work")
        self.assertListEqual([], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Should work")
        # check with a file on more than one disk
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(3, "MP3", 0), 1), "Should work")
        # now:
        #   File 1 on: disks 1
        #   File 2 on: disks 1
        #   File 3 on: disks 1,2
        #   File 4 on: disks None
        self.assertListEqual([2, 3], Solution.getCloseFiles(1), "Should work")
        self.assertListEqual([1, 3], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([1, 2], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Should work")
        # check with violation of 50%
        self.assertEqual(Status.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(3, "MP3", 0), 3), "Should work")

        # now:
        #   File 1 on: disks 1
        #   File 2 on: disks 1
        #   File 3 on: disks 1,2,3
        #   File 4 on: disks None
        self.assertListEqual([2, 3], Solution.getCloseFiles(1), "Should work")
        self.assertListEqual([1, 3], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([], Solution.getCloseFiles(3), "50% violated")
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Should work")
        # check if it includes 50% (> =)
        self.assertEqual(Status.OK, Solution.addDisk(Disk(4, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 2), 2), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(3, "MP3", 3), 4), "Should work")
        # now:
        #   File 1 on: disks 1
        #   File 2 on: disks 1,2
        #   File 3 on: disks 1,2,3,4
        #   File 4 on: disks None
        self.assertListEqual([2, 3], Solution.getCloseFiles(1), "Should work")
        self.assertListEqual([1, 3], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([2], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Should work")
        # check if order is by ID and not by how "close" the files are
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(1, "MP3", 1), 4), "Should work")
        self.assertEqual(Status.OK, Solution.addFileToDisk(File(2, "MP3", 2), 4), "Should work")

        # now:
        #   File 1 on: disks 1,4
        #   File 2 on: disks 1,2,4
        #   File 3 on: disks 1,2,3,4
        #   File 4 on: disks None

        self.assertListEqual([2, 3], Solution.getCloseFiles(1), "Should work")
        self.assertListEqual([1, 3], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([1, 2], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Should work")

        # check how deleting a disk effects the func
        self.assertEqual(Status.OK, Solution.deleteDisk(2), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.deleteDisk(2), "disk 2 already deleted")

        # now:
        #   File 1 on: disks 1,4
        #   File 2 on: disks 1,4
        #   File 3 on: disks 1,3,4
        #   File 4 on: disks None

        self.assertListEqual([2, 3], Solution.getCloseFiles(1), "Should work")
        self.assertListEqual([1, 3], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([1, 2], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getCloseFiles(4), "Should work")

        # check how deleting a file effects the system
        self.assertEqual(Status.OK, Solution.deleteFile(File(1, "MP3", 1)), "Should work")
        self.assertListEqual([3], Solution.getCloseFiles(2), "Should work")
        self.assertListEqual([2], Solution.getCloseFiles(3), "Should work")
        self.assertListEqual([2, 3], Solution.getCloseFiles(4), "Should work")


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
