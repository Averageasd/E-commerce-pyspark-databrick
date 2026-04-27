# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG ecommerce;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS raw;
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS raw.raw_landing
# MAGIC LOCATION  'abfss://ecommerce-raw-data@ngdbrstorage9497.dfs.core.windows.net/'