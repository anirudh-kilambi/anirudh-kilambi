import csv
import time
import tclab
import numpy as np
import matplotlib.pyplot as plt
from scipy.integrate import odeint

n = 300 # number of second time points (5 min)
m = 0.004 # kg
tm = np.linspace(0, n, n+1)

# lab = tclab.setup(connected=False, speedup=5)
#
skip = True

# data
lab = tclab.TCLabModel()
if skip is False:
    # simulate data if no data has been written out yet.
    t1 = {"data" : [{"time (s)" : 0, "Temperature (degC)" : lab.T1}]}
    lab.Q1(50) # set heater to 50%

    for i in range(n):
        data = {}
        print(i)
        time.sleep(1)
        print(lab.T1)
        data = {"time (s)" : i, "Temperature (degC)" : lab.T1}
        t1["data"].append(data)
    lab.close()


    with open("tclab_temps.csv", "w", encoding = "utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames = ["time (s)", "Temperature (degC)"])
        writer.writerows(t1["data"])
        outfile.close()

def tclab_model(T, t):
    U = 5 # heat transfer coefficient, W/m2-k
    m = 0.004 # mass, kg
    A = 0.0012 # cross-sectional area, m2
    Cp = 500 # heat capacity J/kg-k
    T_k = T + 273
    T_inf = 23 + 273
    T_a = 23 + 273
    sigma = 5.67e-8
    epsilon = 0.9
    alpha = 0.01

    dT_dt = (U * A * (T_a - T_k) + epsilon * sigma * A * (T_inf**4 - T_k**4) + alpha * 50)/(m * Cp)

    return dT_dt

T0 = lab.T1
print(f"T0 => [{T0}]")

T_model = odeint(tclab_model, T0, tm)
T_actual = []
with open("tclab_temps.csv", "r") as infile:
    reader = csv.reader(infile)
    for row in reader:
        T_actual.append(row[1])

plt.figure(1)
plt.plot(tm, T_model, "r-", label="Model")
plt.plot(tm, T_actual, "b-", label="Measured")
plt.ylabel("Temperature (degC)")
plt.xlabel("Time (sec)")
plt.yticks(np.linspace(0, 100, 10))
plt.legend()
plt.show()
