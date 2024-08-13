#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Bits & Bytes related humanization.

Copyright (c) 2010 Jason Moiron and Contributors
https://github.com/jmoiron/humanize/blob/master/humanize/filesize.py
"""

suffixes = {
    'decimal': ('kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'),
    'binary': ('KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'),
    'gnu': "KMGTPEZY",
}


def naturalsize(value, binary=False, gnu=False, format='%.1f'):
    """Format a number of byteslike a human readable filesize (eg. 10 kB).  By
    default, decimal suffixes (kB, MB) are used.  Passing binary=true will use
    binary suffixes (KiB, MiB) are used and the base will be 2**10 instead of
    10**3.  If ``gnu`` is True, the binary argument is ignored and GNU-style
    (ls -sh style) prefixes are used (K, M) with the 2**10 definition.
    Non-gnu modes are compatible with jinja2's ``filesizeformat`` filter."""
    if gnu: suffix = suffixes['gnu']
    elif binary: suffix = suffixes['binary']
    else: suffix = suffixes['decimal']

    base = 1024 if (gnu or binary) else 1000
    bytes = float(value)

    if bytes == 1 and not gnu: return '1 Byte'
    elif bytes < base and not gnu: return '%d Bytes' % bytes
    elif bytes < base and gnu: return '%dB' % bytes

    for i,s in enumerate(suffix):
        unit = base ** (i+2)
        if bytes < unit and not gnu:
            return (format + ' %s') % ((base * bytes / unit), s)
        elif bytes < unit and gnu:
            return (format + '%s') % ((base * bytes / unit), s)
    if gnu:
        return (format + '%s') % ((base * bytes / unit), s)
    return (format + ' %s') % ((base * bytes / unit), s)



def build_dict(headers, data):
    """Build a dict with headers and list of data.

    Example::

        build_dict(['foo', 'bar'], [1, 2])
        {'foo': 1, 'bar': 2}

    :param list headers: List of headers
    :param list data: List of data
    """
    return dict(zip(headers, data))

### New sips table validations ###

TABLA_6 = ['NI', 'NV', 'OT', 'PS', 'NE']
TABLA_9 = ['01', '02', '03', '05', '07', '08', '09', '10', '11', '12']
TABLA_17 = ['001','003','004','005','006','007','008','011','012','013','014','015','016','017','018','019','020','021','022','023','024','025']
TABLA_30 = ['01','02','03','04','05']
TABLA_32 = ['1', '2', '3', '4']
TABLA_35 = ['1','2','3','4','6','8','9','A']
TABLA_62 = ['AL', 'AP', 'AS', 'AT', 'BA', 'CM', 'EA', 'ES', 'FT', 'FV', 'GA', 'GB', 'HP', 'IN', 'IT', 'KC', 'LB', 'LC', 'OF', 'PC', 'RA', 'RT', 'SA', 'SC', 'SE', 'SG', 'SM', 'SO', 'TL', 'TR', 'UF', 'UV', 'VI', 'VE']
TABLA_64 = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '99']
TABLA_108 = ['01','02']
TABLA_111 = ['01', '02', '03']
TAULA_PROPIEDAD_CONTADOR_CORRECTOR = ['04', '06', '01', '02', '11', '12', '08', '10']
TAULA_RESULTADO_INSPECCION = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14']
TAULA_PERFIL_CONSUMO = ['01', '02']
TAULA_TIPO_PEAJE = ['11', '12', '13', '21', '22', '23', '24', '25', '26', '1B', '2B', '3B', '4B', '5B', '6B', '31', '32', '33', '34', '35', '41', '42', '43', '44', '45', '46', '47', 'A1', 'A2', 'A3', 'B1', 'B2', 'C1', 'C2', 'D1', 'D2', 'M1', 'M2', 'R1', 'R2', 'R3', 'R4', 'R8', 'R9', 'L0', 'L1', 'A5', 'A6', 'A7', 'B5', 'B6', 'B7', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', '99']
TAULA_TIPO_LECTURA_GAS = ['R', 'E']  # R: real, E: estimada
TAULA_TIPO_LECTURA = ['0', '1', '2']