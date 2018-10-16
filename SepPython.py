
def main():
    pass

if __name__ == '__main__':
    dir = "Out/"
    file = "statistics.log.2018"
    outdir = "SeparatedFiles/"

    file1 = file+"-item"
    file2 = file+"-bitstream"
    file3 = file+"-Communities"
    file4 = file+"-Collection"

    f1 = open(outdir+file1, "w")
    f2 = open(outdir+file2, "w")
    f3 = open(outdir+file3, "w")
    f4 = open(outdir+file4, "w")

    f = open(dir+file, "r")

    for l in f.readlines():
        aux = l.split(",")
        if aux[1] == "view_bitstream":
            f2.write(l)
        elif aux[1] == "view_community":
            f3.write(l)
        elif aux[1] == "view_collection":
            f4.write(l)
        else:
            f1.write(l)


    