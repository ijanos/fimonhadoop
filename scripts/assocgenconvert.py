from __future__ import print_function
import sys


curr_items = []
curr_transid = None

if __name__ == "__main__":
    for line in sys.stdin:
        segments = line.strip().split()
        trid = segments[0]
        item = segments[2]
        if trid != curr_transid:
            if curr_transid is not None:
                print(curr_transid, " ".join(curr_items))
            curr_transid = trid
            curr_items = [item]
        else:
            curr_items.append(item)

    # flush last transaction
    print(curr_transid, " ".join(curr_items))
