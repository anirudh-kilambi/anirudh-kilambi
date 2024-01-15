import numpy as np
import matplotlib.pyplot as plt
from scipy.integrate import odeint


V = 100 # m3
q = 100 # m3/hr

# z = [Ca, T]
# dz = [dCa_dt, dT_dt]
def func(z, t):
    q = 100 # volumetric flow rate -- 100 m^3/hr
    V = 100 # volume of vessel, 100 m^3
    Ca = z[0] # concentration
    Ca_f = 1 # inlet concentration
    T = z[1] # temperature at outlet
    Tf = 300 # inlet temperature
    dCa_dt = q/V * (Ca_f - Ca)
    dT_dt = q/V * (Tf - T)

    return [dCa_dt, dT_dt]

t = np.linspace(0, 10, 100)


z0 = [0, 350] # z[0] = Ca0, z[1] = T0


z = odeint(func, z0, t)
Ca = z[:, 0]
T = z[:, 1]

fig, axes = plt.subplots(2, 1, layout="constrained")
axes[0].plot(t, Ca, "r--")
axes[0].set_ylim(0, 1)
axes[0].set_xlabel("Time (hr)")
axes[0].set_ylabel("Concentration of Species A (M)")
axes[0].grid(True)

axes[1].plot(t, T, "b--")
axes[1].set_ylim(300, 350)
axes[1].set_xlabel("Time (hr)")
axes[1].set_ylabel("Temperature at outlet (K)")
axes[1].grid(True)

plt.show()
