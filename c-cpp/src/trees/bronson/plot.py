import os
import matplotlib.pyplot as plt

if not os.path.exists("plots"):
    os.mkdir("plots")

log_files = os.listdir("logs/")

for f in log_files:
    log = open("logs/%s" % f, "r")
    x = [int(line) for line in log.readlines()]

    if len(x) < 10:
        continue

    x.sort()
    margin = int(0.015 * len(x))
    x = x[margin:len(x) - margin]

    r = (x[0], x[-1])

    plt.clf()
    n, bits, patches = plt.hist(x, 100, range=r)

    plot_file = "plots/" + f.split(".")[0] + ".png"
    plt.savefig(plot_file)
