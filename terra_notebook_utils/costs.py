from math import ceil


MINUTE = 60

class GCPCustomN1Cost:
    # Pricing information: https://cloud.google.com/compute/vm-instance-pricing#n1_custommachinetypepricing
    on_demand_per_hour_per_cpu = 0.033174
    on_demand_per_hour_per_gb = 0.004446
    preemptible_per_hour_per_cpu = 0.00698
    preemptible_per_hour_per_gb = 0.00094

    @classmethod
    def estimate(cls, cpus: int, memory_gb: int, runtime_seconds: float, preemptible: bool) -> float:
        # GCP Instance Billing Model:
        # https://cloud.google.com/compute/vm-instance-pricing#billingmodel
        if preemptible:
            per_hour_per_cpu = cls.preemptible_per_hour_per_cpu
            per_hour_per_gb = cls.preemptible_per_hour_per_gb
        else:
            per_hour_per_cpu = cls.on_demand_per_hour_per_cpu
            per_hour_per_gb = cls.on_demand_per_hour_per_gb
        cost_duration_hours = ceil(MINUTE if MINUTE >= runtime_seconds else runtime_seconds) / 3600
        cost_for_cpus = cost_duration_hours * cpus * per_hour_per_cpu
        cost_for_mem = cost_duration_hours * memory_gb * per_hour_per_gb
        cost = cost_for_cpus + cost_for_mem
        return cost
