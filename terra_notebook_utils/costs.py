from math import ceil


SECOND = 1.0
MINUTE = SECOND * 60
HOUR = MINUTE * 60
DAY = HOUR * 24
MONTH = DAY * 30.437  # Not critical to be exact

class PersistentDisk:
    """GCP persistent disk costs: https://cloud.google.com/compute/disks-image-pricing#persistentdisk"""
    standard = 0.04 / MONTH  # GCP disk prices are quoted per month :(

    @classmethod
    def estimate(cls, size_gb: float, runtime_seconds: float) -> float:
        return cls.standard * size_gb * runtime_seconds

class GCPCustomN1Cost:
    # Pricing information: https://cloud.google.com/compute/vm-instance-pricing#n1_custommachinetypepricing
    on_demand_per_cpu = 0.033174 / HOUR
    on_demand_per_gb = 0.004446 / HOUR
    preemptible_per_cpu = 0.00698 / HOUR
    preemptible_per_gb = 0.00094 / HOUR

    @classmethod
    def estimate(cls, cpus: int, memory_gb: float, runtime_seconds: float, preemptible: bool) -> float:
        # GCP Instance Billing Model:
        # https://cloud.google.com/compute/vm-instance-pricing#billingmodel
        if preemptible:
            per_cpu = cls.preemptible_per_cpu
            per_gb = cls.preemptible_per_gb
        else:
            per_cpu = cls.on_demand_per_cpu
            per_gb = cls.on_demand_per_gb
        cost_duration_hours = ceil(MINUTE if MINUTE >= runtime_seconds else runtime_seconds)
        cost_for_cpus = cost_duration_hours * cpus * per_cpu
        cost_for_mem = cost_duration_hours * memory_gb * per_gb
        cost = cost_for_cpus + cost_for_mem
        return cost
