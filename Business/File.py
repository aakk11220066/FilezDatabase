class File:
    def __init__(self, fileID=None, type=None, size=None):
        self.__fileID = fileID
        self.__type = type
        self.__size = size

    def getFileID(self):
        return self.__fileID

    def setFileID(self, fileID):
        self.__fileID = fileID

    def getType(self):
        return self.__type

    def setType(self, type):
        self.__type = type

    def getSize(self):
        return self.__size

    def setSize(self, size):
        self.__size = size

    @staticmethod
    def badFile():
        return File()

    def __str__(self):
        print("fileID=" + str(self.__fileID) + ", type=" + str(self.__type) + ", size=" + str(self.__size))
