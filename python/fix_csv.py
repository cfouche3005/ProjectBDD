import csv
import ftfy
import sys

def main(argv):
    # input file
    csvfile = open(argv[1], "r", encoding = "Windows-1252")
    reader = csv.DictReader(csvfile)

    # output stream
    outfile = open(argv[2], "w", encoding = "UTF8") # Windows doesn't like utf8
    writer = csv.DictWriter(outfile, fieldnames = reader.fieldnames, lineterminator = "\n")

    # clean values
    writer.writeheader()
    for row in reader:
        for col in row:
            row[col] = ftfy.fix_text(row[col])
            row[col] = row[col].replace("ÃŽle", "Île")
            row[col] = row[col].replace("\\", "")
        writer.writerow(row)

    # close files
    csvfile.close()
    outfile.close()

if __name__ == "__main__":
    main(sys.argv)