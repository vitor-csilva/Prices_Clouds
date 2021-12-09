# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class Machine(models.Model):
    machine_name = models.CharField(max_length=256, blank=True, null=True)
    memory = models.CharField(max_length=256, blank=True, null=True)
    cpu = models.CharField(max_length=256, blank=True, null=True)
    storage = models.CharField(max_length=256, blank=True, null=True)
    processor = models.CharField(max_length=256, blank=True, null=True)
    colection_data = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'machine'


class SystemMachine(models.Model):
    machine = models.ForeignKey(Machine, models.DO_NOTHING, blank=True, null=True)
    price = models.CharField(max_length=256, blank=True, null=True)
    region = models.CharField(max_length=256, blank=True, null=True)
    system_name = models.CharField(max_length=256, blank=True, null=True)
    description = models.CharField(max_length=256, blank=True, null=True)
    type_machine = models.CharField(max_length=256, blank=True, null=True)
    lease_contract_length = models.CharField(max_length=256, blank=True, null=True)
    offering_class = models.CharField(max_length=256, blank=True, null=True)
    purchase_option = models.CharField(max_length=256, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'system_machine'
