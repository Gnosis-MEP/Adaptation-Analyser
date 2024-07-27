#!/usr/bin/env python
from unittest.mock import MagicMock
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401,E402
import skfuzzy as fuzz
from skfuzzy import control as ctrl

from ua_analysis import UAServiceAnalysis


parent_service = MagicMock()
adaptation_delta = 10
parent_service.adaptation_delta = adaptation_delta


workers = {
    'worker1': {"throughput": 10,},
    'worker2': {"throughput": 100,},
}

usage_analysis = UAServiceAnalysis(parent_service=parent_service, service_type="SomeService")


usage_analysis.setup_from_workers(workers)


num_steps = 41
upsampled = np.linspace(0, usage_analysis.sw_max_cap, num_steps)
x, y = np.meshgrid(upsampled, upsampled)
z = np.zeros_like(x)
# Loop through the system 21*21 times to collect the control surface
for i in range(num_steps):
    for j in range(num_steps):
        # print(f'compute with: mc: {x[i, j]};  qs: {y[i, j]}')
        try:
            z[i, j] = usage_analysis.calculate_worker_usage(y[i, j], x[i, j])
        except Exception as e:
            usage_analysis.sim.print_state()
            raise e

# Plot the result in pretty 3D with alpha blending

fig = plt.figure(figsize=(8, 8))
ax = fig.add_subplot(111, projection='3d')

surf = ax.plot_surface(x, y, z, rstride=1, cstride=1, cmap='viridis',
                       linewidth=0.4, antialiased=True)

# not sure what this is, so wont mess  with it
# cset = ax.contourf(x, y, z, zdir='z', offset=3, cmap='viridis', alpha=0.5)
# cset = ax.contourf(x, y, z, zdir='x', offset=3, cmap='viridis', alpha=0.5)
# cset = ax.contourf(x, y, z, zdir='y', offset=3, cmap='viridis', alpha=0.5)
ax.set_zlabel('usage %', rotation=90)
ax.set_xlabel('Max Cap.')
ax.set_ylabel('Queue Size')
ax.view_init(30, -50)
plt.savefig(f'sims_output.png')





del x, y, z
num_steps = 41
upsampled = np.linspace(0, usage_analysis.sw_max_cap, num_steps)
x, y = np.meshgrid(upsampled, upsampled)
z = np.zeros_like(x)
# Loop through the system 21*21 times to collect the control surface
for i in range(num_steps):
    for j in range(num_steps):
        if x[i, j] == 0 or y[i, j] == 0:
            z[i, j] = 0
        else:
            usage = (y[i, j] / x[i, j]) * 100
            if usage > 100:
                usage = 100
            z[i, j] = usage
        print(f'compute with: mc: {x[i, j]};  qs: {y[i, j]} = {z[i, j]}')

# Plot the result in pretty 3D with alpha blending

fig = plt.figure(figsize=(8, 8))
ax = fig.add_subplot(111, projection='3d')

surf = ax.plot_surface(x, y, z, rstride=1, cstride=1, cmap='viridis',
                       linewidth=0.4, antialiased=True)

# not sure what this is, so wont mess  with it
# cset = ax.contourf(x, y, z, zdir='z', offset=3, cmap='viridis', alpha=0.5)
# cset = ax.contourf(x, y, z, zdir='x', offset=3, cmap='viridis', alpha=0.5)
# cset = ax.contourf(x, y, z, zdir='y', offset=3, cmap='viridis', alpha=0.5)
ax.set_zlabel('usage %', rotation=90)
ax.set_xlabel('Max Cap.')
ax.set_ylabel('Queue Size')
ax.view_init(30, -50)

plt.savefig(f'crisp_output.png')
# plt.savefig(f'sims_output2.png')
