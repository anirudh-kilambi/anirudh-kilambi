import numpy as np
import matplotlib.pyplot as plt
from math import floor

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
# initialize values taken from the CPU numerical approximation
T0 = T_interface # k - initial temperature at cpu-heatsink interface
print(T0)

# initialize vonstants
T_inf = 298 # temperature of the ambient air (k)
L = 0.015 # length (height of cooling fin) -> m
k_fin = 389.86 # w/m k -> Conduction coefficient of cooling fin
h_air = 10 # convective heat transfer coefficient of air -> w/m2 K
thickness_fin = 0.002 # thickness of fin -> m
W_fin = 0.0375 # width of fin -> m
volumetric_price_of_copper = 288881.36 # USD/m3

def number_of_fins(thickness, spacing=0.001):
    """
    This function handles the number of fins possible given a certain fin thickness.
    Spacing required between fins is 0.001 m
    """
    length_total = 0.0375 - thickness
    thickness_total = thickness + spacing # this is the spacing for one fin and it's required space
    number_of_fins = length_total/thickness_total 
    return floor(number_of_fins + 1)


n = 21 # total number of nodes (19 internal + 1 at each boundary)
dy = L/(n-1) # distance between each internal node

counter = 0

no_fins = number_of_fins(thickness_fin)
V_fin = W_fin * thickness_fin * L * no_fins # total volume of Copper used
cost_of_materials = V_fin * volumetric_price_of_copper
print(f"COST OF MATERIALS: {cost_of_materials}")
P = ((2 * W_fin) + (2 * thickness_fin)) * no_fins # perimeter of area perpendicular to the heat transfer
A = W_fin * thickness_fin * no_fins # area perpendicular to the heat transfer through the fin
T = np.zeros((n,)) # initialize a temperature array of zeros
T[0] = T0 # apply the initial condition of the temperature 

theta = ((P*h_air)/(k_fin*A)) # from the writing of the finite differences, the central diagonal has a hP/kA term, combining constants to simplify for later

# initialize the matrix of finite differences
A=np.diag((2+theta*dy**2)*np.ones(n-1)) +\
        np.diag(-1*np.ones(n-2),1)+\
        np.diag(-1*np.ones(n-2),-1)
A[-1, -2] = -2  # as a result of the boundary condition at the tip of the fin, the final finite difference is -2Tn-1 + (2 x theta)Tn

b=(theta)*dy**2*T_inf*np.ones(n-1) # initialize the B array
b[0] += T0 # add the initial temperature to the first value as a result of the boundary condition

T[1:] = np.linalg.solve(A,b) # solve for the Temperatures at each node
print(f"MINIMUM TEMPERATURE: {T[-1]}")

yvals = np.linspace(0.002, L, n)
print(yvals)
plt.figure()
plt.plot(yvals, T, "r-", label = "Temperature Profile of Cooling Fins")
plt.plot(yvals_cpu[:-1], T_cpu[:-1], "b-",label="Temperature Profile of CPU" )
plt.plot(yvals_cpu[-1], T_cpu[-1], "g*", label = "Temperature at Interface")
plt.legend()
plt.xlabel("Length (m)")
plt.ylabel("Temperature (K)")
plt.title("Temperature Profile of Overall System")
plt.show()

