import numpy as np
import matplotlib.pyplot as plt

L_cpu = 0.0375
W_cpu = L_cpu
H_cpu = 0.002
gen_cpu = 65
k_cpu = 10

A_cpu = L_cpu * W_cpu # area of the CPU perpendicular to the Y direction (face of CPU)
V_cpu = L_cpu * W_cpu * H_cpu # determine the volume of the CPU
q_dot_cpu = gen_cpu/V_cpu # determine the volumetric rate of generation through the CPU


n = 100
dy = H_cpu / (n-1) # determine the size of spacing

theta =  (-q_dot_cpu/k_cpu) * dy**2 # for the finite differences, there is a common equation that will be seen. Abstracted for simlification

T_cpu = np.zeros((n,))
T_base_CPU = 400 # dummy initial temperature to enter while loop
counter = 0 
T_end = 383 # define an arbitrary end temperature, will find the actual one once the Base temperature is reached
while T_base_CPU > 373:
    if counter > 0:
        T_end -= 1 # since base temperature is greater than 373 K, will need to decrease the temperature at the interface to reach 373 K at base 
    else:
        T_end = T_end

    # initialize a matrix
    A = np.diag(-2 * np.ones(n)) + np.diag(1 * np.ones(n-1), 1) + np.diag(1 * np.ones(n-1), -1)
    # Update coefficients at boundaries
    A[0, 1] = 2
    A[-1, -1] = 1
    A[-1, -2] = 0

    # initialize b array
    b = theta * np.ones(n)
    b[-1] = T_end # update final b value for boundary condition

    T_cpu = np.linalg.solve(A, b)
    T_base_CPU = T_cpu[0]
    # T_interface = T[-1]
    counter += 1

yvals_cpu = np.linspace(0, H_cpu, n) # initialize y values for graph

T_interface = T_cpu[-1] # initialize temperature variable for import into fin.py

plt.figure()
plt.plot(yvals_cpu, T_cpu, "b-")
plt.legend()
plt.title("Temperature vs Distance through the CPU in the y direction")
plt.xlabel('Distance in y direction (m)')
xticks = np.linspace(0, H_cpu, 5)
plt.xticks(xticks)
plt.ylabel('T (K)')
plt.show()


