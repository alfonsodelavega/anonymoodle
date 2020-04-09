#!/usr/bin/env python
################################################################################
# Usage
################################################################################
# ./anonymoodle.py log_name
# or
# python anonymoodle.py log_name
################################################################################
# Arguments
################################################################################
# log_name : CSV moodle log file
################################################################################
# Output
# {log_name}_anonymized.csv : Log without original names
# {log_name}_{sufixxes} : Maps each changed column with original (several files)
################################################################################

# %%
import pandas as pd
import sys
import re
from random import shuffle
from random import randint
from faker import Faker
from collections import defaultdict

# %%
n_original_name = "NombreOriginal"
n_name = "Nombre completo del usuario"
n_original_id = "IdUsuarioOriginal"
n_userid = "IdUsuario"
n_description = "Descripción"
n_affected_user = "Usuario afectado"
n_event = "Nombre evento"
n_origin = "Origen"
n_ip = "Dirección IP"
n_datetime = "Hora"
n_original_component = "ComponenteOriginal"
n_component = "Componente"
n_original_context = "ContextoOriginal"
n_context = "Contexto del evento"

n_calendar_events = "Evento de calendario actualizado"

id_pattern = "The user with id '(-?\\d+)'"
null_id = -288

def get_user_id(row):
    pattern = re.match(id_pattern, row[n_description])
    if pattern is not None:
        return pattern.group(1)
    return null_id


def get_fake_names(length):
    names = set()
    fake = Faker("es_ES")
    while len(names) < length:
        names.add(fake.name())
    return list(names)


def anonymize_description(description, ids_dict):
    return re.sub("'(-?\\d+)'",
                  lambda match: "'{}'".format(ids_dict[match.group(1)]),
                  description)


def anonymize_log(df):
    # Get user ids, and remove those lines without it
    df[n_userid] = df.apply(get_user_id, axis=1)
    df = df[~df[n_userid].isin([null_id])].copy()

    # Swap original names with a random one
    original_names = set()
    original_names.update(df[n_name].unique())
    original_names.update(df[n_affected_user].unique())
    original_names = list(original_names)
    shuffle(original_names)
    names_dict = dict(zip(original_names,
                          get_fake_names(len(original_names))))
    names_dict["-"] = "-"  # for non-present names in the affected user column
    df[n_original_name] = df[n_name]
    df[n_name] = df[n_name].apply(lambda name: names_dict[name])
    df[n_affected_user] =\
        df[n_affected_user].apply(lambda name: names_dict[name])


    # Swap original ids (these might match the moodle server)
    original_ids = set()
    original_ids.update(df[n_userid].unique())
    original_ids = list(original_ids)
    shuffle(original_ids)
    ids_dict = defaultdict(lambda:null_id,
                           zip(original_ids, range(len(original_ids))))
    # Leave this default/admin ids without change
    ids_dict["-1"] = "-1"
    ids_dict["0"] = "0"
    ids_dict["1"] = "1"
    ids_dict["2"] = "2"

    df[n_original_id] = df[n_userid]
    df[n_userid] = df[n_userid].apply(lambda uid: ids_dict[uid])
    # Calendar events contain unwanted information in the description column
    #   Remove those columns
    df = df[df[n_event] != n_calendar_events].copy()
    # Also, swap ids from the description text (trickier)
    df[n_description] = df[n_description].apply(
            lambda d : anonymize_description(d, ids_dict))

    # Swap course details
    original_contexts = set()
    original_contexts.update(df[n_context].unique())
    original_contexts = list(original_contexts)
    shuffle(original_contexts)
    context_dict = dict(zip(original_contexts,
                            ["Context {}".format(i)
                             for i in range(len(original_contexts))]))
    df[n_original_context] = df[n_context]
    df[n_context] = df[n_context].apply(lambda c: context_dict[c])

    mappings = (("_ids.csv",
                 df[[n_original_id, n_userid]].copy().drop_duplicates()),
                ("_names.csv",
                 df[[n_original_name, n_name]].copy().drop_duplicates()),
                ("_contexts.csv",
                 df[[n_original_context, n_context]].copy().drop_duplicates()))

    # Swap IPs
    df[n_ip] = df[n_ip].apply(
            lambda _: "{}.{}.{}.{}".format(randint(0, 255), randint(0, 255),
                                           randint(0, 255), randint(0, 255)))

    df = df.drop([n_original_id, n_original_name, n_original_context], axis=1)
    return df, mappings


# %%
# Main Program
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ./anonymize-log.py log_name")
        exit(1)
    log_name = sys.argv[1]
    df = pd.read_csv(log_name)
    df, mappings = anonymize_log(df)
    # Output generated files
    output_log_file = "{}_anonymized.csv".format(log_name[:-4])
    df.to_csv(output_log_file, index=False)
    for suffix, df_map in mappings:
        df_map.to_csv("{}{}".format(log_name[:-4], suffix), index=False)
