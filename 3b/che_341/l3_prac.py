import numpy as np
import matplotlib.pyplot as plt
from scipy.integrate import odeint


def model(y, t):
    dydt = -y + 1

    return dydt

t = np.linspace(0, 10)

y0 = 0


y = odeint(func=model, y0=y0, t=t)

plt.plot(t, y)
plt.xlabel("time")
plt.ylabel("y(t)")
plt.show()
