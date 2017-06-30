#!/usr/bin/python
# encoding: utf-8


__file__ = '$'
__author__ = 'szj0306'  # 志杰
__date__ = '6/30/17 3:33 PM'
__license__ = "Public Domain"
__version__ = '$'
__email__ = "jason860306@gmail.com"
# '$'


class GeneralHash(object):
    """

    """
    @staticmethod
    def RSHash(key):
        a = 378551
        b = 63689
        hash_value = 0
        for i in range(len(key)):
            hash_value = hash_value * a + ord(key[i])
            a = a * b
        return hash_value & 0x7FFFFFFF

    @staticmethod
    def JSHash(key):
        hash_value = 1315423911
        for i in range(len(key)):
            hash_value ^= ((hash_value << 5) + ord(key[i]) + (hash_value >> 2))
        return hash_value & 0x7FFFFFFF

    @staticmethod
    def PJWHash(key):
        BitsInUnsignedInt = 4 * 8
        ThreeQuarters = long((BitsInUnsignedInt * 3) / 4)
        OneEighth = long(BitsInUnsignedInt / 8)
        HighBits = 0xFFFFFFFF << (BitsInUnsignedInt - OneEighth)
        hash_value = 0
        test = 0
        for i in range(len(key)):
            hash_value = (hash_value << OneEighth) + ord(key[i])
            test = hash_value & HighBits
            if test != 0:
                hash_value = ((hash_value ^ (test >> ThreeQuarters)) & (~HighBits))
        return hash_value & 0x7FFFFFFF

    @staticmethod
    def ELFHash(key):
        hash_value = 0
        x = 0
        for i in range(len(key)):
            hash_value = (hash_value << 4) + ord(key[i])
            x = hash_value & 0xF0000000
            if x != 0:
                hash_value ^= (x >> 24)
                hash_value &= ~x
        return hash_value & 0x7FFFFFFF

    @staticmethod
    def BKDRHash(key):
        seed = 131  # 31 131 1313 13131 131313 etc..
        hash_value = 0
        for i in range(len(key)):
            hash_value = (hash_value * seed) + ord(key[i])
        return hash_value & 0x7FFFFFFF

    @staticmethod
    def SDBMHash(key):
        hash_value = 0
        for i in range(len(key)):
            hash_value = ord(key[i]) + (hash_value << 6) + \
                         (hash_value << 16) - hash_value
        return hash_value & 0x7FFFFFFF

    @staticmethod
    def DJBHash(key):
        hash_value = 5381
        for i in range(len(key)):
            hash_value = ((hash_value << 5) + hash_value) + ord(key[i])
        return hash_value & 0x7FFFFFFF

    @staticmethod
    def DEKHash(key):
        hash_value = len(key)
        for i in range(len(key)):
            hash_value = ((hash_value << 5) ^ (hash_value >> 27)) ^ ord(key[i])
        return hash_value & 0x7FFFFFFF

    @staticmethod
    def APHash(key):
        hash_value = 0
        for i in range(len(key)):
            if (i & 1) == 0:
                hash_value ^= ((hash_value << 7) ^ ord(key[i]) ^ (hash_value >> 3))
            else:
                hash_value ^= (~((hash_value << 11) ^ ord(key[i]) ^ (hash_value >> 5)))
        return hash_value & 0x7FFFFFFF


if __name__ == "__main__":
    print GeneralHash.RSHash('abcdefghijklmnopqrstuvwxyz1234567890')
    print GeneralHash.JSHash('abcdefghijklmnopqrstuvwxyz1234567890')
    print GeneralHash.PJWHash('abcdefghijklmnopqrstuvwxyz1234567890')
    print GeneralHash.ELFHash('abcdefghijklmnopqrstuvwxyz1234567890')
    print GeneralHash.BKDRHash('abcdefghijklmnopqrstuvwxyz1234567890')
    print GeneralHash.SDBMHash('abcdefghijklmnopqrstuvwxyz1234567890')
    print GeneralHash.DJBHash('abcdefghijklmnopqrstuvwxyz1234567890')
    print GeneralHash.DEKHash('abcdefghijklmnopqrstuvwxyz1234567890')
    print GeneralHash.APHash('abcdefghijklmnopqrstuvwxyz1234567890')
