
from argparse import ArgumentParser, FileType, Action
import sys
import codecs
import os

# example use: FileFormatConverter.py --srcfile=radkfile --srcformat=euc-jp --dstfile=radkfile.txt --dstformat=utf8

def parse_cmdline():
    parser = ArgumentParser()
    parser.add_argument("--srcfile", help="path to the file to convert", required=True)
    parser.add_argument("--srcformat", help="format of the source file", required=True)
    parser.add_argument("--dstfile", help="path to the converted file", required=True)
    parser.add_argument("--dstformat", help="format of the destination file", required=True)
    return parser.parse_args()


def main():
    args = parse_cmdline()
    linecounter = 0

    print("Conversion from file '%s' (%s) to file '%s' (%s)" % (args.srcfile, args.srcformat, args.dstfile, args.dstformat))

    srcfile = codecs.open(args.srcfile, 'r', args.srcformat)
    dstfile = codecs.open(args.dstfile, 'w', args.dstformat)

    for line in srcfile:
        dstfile.write(line)
        linecounter += 1

    print("Converted %s lines" % linecounter)

    srcfile.close()
    dstfile.close()
    
if __name__ == '__main__':
    main()

