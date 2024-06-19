import numpy as np
import matplotlib.pyplot as plt
import skfuzzy as fuzz
from skfuzzy import control as ctrl

class UAServiceAnalysis(object):
    def __init__(self):
        pass

adaptation_delta = 10
workers_throughput = [50, 100]
sw_max_throughput =  max(workers_throughput)
sw_max_cap = sw_max_throughput * adaptation_delta
service_universe = np.arange(0, sw_max_cap + 1, 1)
usage_universe = np.arange(0, 100 + 1, 1)

names = ['very_low', 'low', 'medium', 'high', 'very_high']

adp_max_cap = ctrl.Antecedent(service_universe, 'max_capacity')
adp_max_cap.automf(names=names)
adp_max_cap.view()
plt.savefig('adp_max_cap.png')

queue_size = ctrl.Antecedent(service_universe, 'queue_size')
queue_size.automf(names=names)
queue_size.view()
plt.savefig('queue_size.png')

usage = ctrl.Consequent(usage_universe, 'usage')
usage.automf(names=names)
usage.view()
plt.savefig('usage.png')

# qsize: very_low
rule1 = ctrl.Rule(queue_size['very_low'], usage['very_low'])
rule1.view()
plt.savefig('rule1.png')

# qsize: low
rule2 = ctrl.Rule((adp_max_cap['very_low'] | adp_max_cap['low'] | adp_max_cap['medium']) & queue_size['low'], usage['low'])
rule3 = ctrl.Rule((adp_max_cap['high'] | adp_max_cap['very_high']) & queue_size['low'], usage['very_low'])

# qsize: medium
rule4 = ctrl.Rule((adp_max_cap['very_low'] | adp_max_cap['low'] ) & queue_size['medium'], usage['high'])
rule5 = ctrl.Rule(adp_max_cap['medium'] & queue_size['medium'], usage['medium'])
rule6 = ctrl.Rule(adp_max_cap['high'] & queue_size['medium'], usage['low'])
rule7 = ctrl.Rule(adp_max_cap['very_high'] & queue_size['medium'], usage['very_low'])
rule4.view()
plt.savefig('rule4.png')

# qsize: high
rule8 = ctrl.Rule(adp_max_cap['very_low'] & queue_size['high'], usage['very_high'])
rule9 = ctrl.Rule(adp_max_cap['low'] & queue_size['high'], usage['high'])
rule10 = ctrl.Rule((adp_max_cap['medium'] | adp_max_cap['high'])& queue_size['high'], usage['medium'])
rule11 = ctrl.Rule(adp_max_cap['very_high'] & queue_size['high'], usage['low'])

# qsize: very_high
rule12 = ctrl.Rule(adp_max_cap['very_low'] & queue_size['very_high'], usage['very_high'])
rule13 = ctrl.Rule((adp_max_cap['low'] | adp_max_cap['medium']) & queue_size['very_high'], usage['high'])
rule14 = ctrl.Rule((adp_max_cap['high'] | adp_max_cap['very_high']) & queue_size['very_high'], usage['medium'])

# rules = [globals()[f'rule{i}'] for i in range(1, 14+1)]
rules = []

# rules for queue_size very_low
rules.append(
    ctrl.Rule(queue_size['very_low'], usage['very_low'])
)

# rules for queue_size low
rules.append(
    ctrl.Rule(adp_max_cap['very_low'] & queue_size['low'], usage['high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['low'] & queue_size['low'], usage['medium'])
)
rules.append(
    ctrl.Rule((adp_max_cap['medium'] | adp_max_cap['high']) & queue_size['low'], usage['low'])
)
rules.append(
    ctrl.Rule(adp_max_cap['very_high'] & queue_size['low'], usage['very_low'])
)

# rules for queue_size medium
rules.append(
    ctrl.Rule(adp_max_cap['very_low'] & queue_size['medium'], usage['very_high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['low'] & queue_size['medium'], usage['high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['medium'] & queue_size['medium'], usage['high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['high'] & queue_size['medium'], usage['medium'])
)
rules.append(
    ctrl.Rule(adp_max_cap['very_high'] & queue_size['medium'], usage['low'])
)

# rules for queue_size high
rules.append(
    ctrl.Rule(adp_max_cap['very_low'] & queue_size['high'], usage['very_high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['low'] & queue_size['high'], usage['very_high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['medium'] & queue_size['high'], usage['very_high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['high'] & queue_size['high'], usage['high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['very_high'] & queue_size['high'], usage['medium'])
)

# rules for queue_size very_high
rules.append(
    ctrl.Rule(adp_max_cap['very_low'] & queue_size['very_high'], usage['very_high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['low'] & queue_size['very_high'], usage['very_high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['medium'] & queue_size['very_high'], usage['very_high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['high'] & queue_size['very_high'], usage['very_high'])
)
rules.append(
    ctrl.Rule(adp_max_cap['very_high'] & queue_size['very_high'], usage['high'])
)


usage_ctrl = ctrl.ControlSystem(rules)



class FixedControlSystemSimulation(ctrl.ControlSystemSimulation):

    def print_state(self):
        from skfuzzy.control import controlsystem
        """
        Print info about the inner workings of a ControlSystemSimulation.
        """
        if next(self.ctrl.consequents).output[self] is None:
            raise ValueError("Call compute method first.")

        print("=============")
        print(" Antecedents ")
        print("=============")
        for v in self.ctrl.antecedents:
            print("{0} = {1}".format(v, v.input[self]))
            for term in v.terms.values():
                print("  - {0: <32}: {1}".format(term.label,
                                                term.membership_value[self]))
        print("")
        print("=======")
        print(" Rules ")
        print("=======")
        rule_number = {}
        for rn, r in enumerate(self.ctrl.rules):
            assert isinstance(r, ctrl.Rule)
            rule_number[r] = "RULE #%d" % rn
            print("RULE #%d:\n  %s\n" % (rn, r))

            print("  Aggregation (IF-clause):")
            for term in r.antecedent_terms:
                assert isinstance(term, controlsystem.Term)
                print("  - {0}: {1}".format(term.full_label,
                                                term.membership_value[self]))
            print("    {0} = {1}".format(r.antecedent,
                                             r.aggregate_firing[self]))

            print("  Activation (THEN-clause):")
            for c in r.consequent:
                assert isinstance(c, controlsystem.WeightedTerm)
                print("    {0} : {1}".format(c,
                                                 c.activation[self]))
            print("")
        print("")

        print("==============================")
        print(" Intermediaries and Conquests ")
        print("==============================")
        for c in self.ctrl.consequents:
            print("{0} = {1}".format(
                c, controlsystem.CrispValueCalculator(c, self).defuzz()))

            for term in c.terms.values():
                print("  %s:" % term.label)
                for cut_rule, cut_value in term.cuts[self].items():
                    if cut_rule not in rule_number.keys():
                        continue
                    print("    {0} : {1}".format(rule_number[cut_rule],
                                                     cut_value))
                accu = "Accumulate using %s" % c.accumulation_method.__name__
                print("    {0} : {1}".format(accu,
                                                 term.membership_value[self]))
            print("")

usage_analysis = FixedControlSystemSimulation(usage_ctrl)
# mc_input = 35 * adaptation_delta
# qs_input = 350
# usage_analysis.input['max_capacity'] = mc_input
# usage_analysis.input['queue_size'] = qs_input
# usage_analysis.compute()
# print(usage_analysis.output['usage'])
# usage.view(sim=usage_analysis)
# plt.savefig(f'u{sw_max_cap}_mc{mc_input}_qs_{qs_input}_output.png')

# usage_analysis.print_state()




upsampled = np.linspace(0, sw_max_cap, 21)
x, y = np.meshgrid(upsampled, upsampled)
z = np.zeros_like(x)
sim = usage_analysis
# Loop through the system 21*21 times to collect the control surface
for i in range(21):
    for j in range(21):
        sim.input['max_capacity'] = x[i, j]
        sim.input['queue_size'] = y[i, j]
        print(f'compute with: mc: {x[i, j]};  qs: {y[i, j]}')
        try:
            sim.compute()
        except Exception as e:
            sim.print_state()
            raise e
        z[i, j] = sim.output['usage']

# Plot the result in pretty 3D with alpha blending
import matplotlib.pyplot as plt  # noqa: E402

# Required for 3D plotting
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401,E402

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
# plt.savefig(f'sims_output2.png')
