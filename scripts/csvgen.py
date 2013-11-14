#!/usr/bin/env python

INPUTS = [
    ("/hduser/apriori/10m", 10000000),
    ("/hduser/apriori/20m", 20000000)
    ]

MINSUPS = [0.05, 0.01, 0.005]

OUTPUT = "/hduser/out"


if __name__ == "__main__":
    id = 1

    # header line
    print("id,input,outputpath,size,minsup")

    for minsup in MINSUPS:
        for path, size in INPUTS:
            print(",".join([str(x) for x in (id, path, OUTPUT, size, minsup)]))
            id += 1
