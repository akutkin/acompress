#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 15:51:06 2020

@authors: Alexander and Mattia
"""

import logging
from argparse import ArgumentParser

_POOL_TIME = 10 # SECONDS
_MAX_COMPRESS_TIME = 24 * 3600 # HOURS * SECONDS
_MAX_POOL = _MAX_COMPRESS_TIME // _POOL_TIME

from casacore.tables import table as CasacoreTable
import shutil
import os
from subprocess import Popen as Process, TimeoutExpired, PIPE
import numpy as np


def setup_logging(verbose=False):
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)


def parse_args():
    parser = ArgumentParser(description='Apply flags and compress measurement set with dysco')
    parser.add_argument('-i', '--input', help='input MS')
    parser.add_argument('-o', '--output', default='', help='output MS (if empty -- the input is overwritten)')
    parser.add_argument('-f', '--flags', help='flag table to restore')
    parser.add_argument('-b', '--bitrate', default=12, help='bitrate for dysco compression')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-d', '--decompress', default=False, action='store_true')
    return parser.parse_args()


def apply_flags(msin_path, flags_path, msout_path=''):
    if not msout_path:
        msout_path = msin_path
    else:
        if os.path.exists(msout_path):
            shutil.rmtree(msout_path)
        shutil.copytree(msin_path, msout_path)
    logging.debug('Applying flags to %s', msout_path)
    with CasacoreTable(msout_path, readonly=False) as table:
        flag_in = CasacoreTable(flags_path)
        table.putcol('FLAG', flag_in.getcol('FLAG'))
        table.putcol('FLAG_ROW', flag_in.getcol('FLAG_ROW'))
    return msout_path


def test_same_flags(tab1, tab2):
    res = np.array_equal(CasacoreTable(tab1).FLAG, CasacoreTable(tab2).FLAG)
    if res:
        logging.info('Flags are the same in %s and %s', tab1, tab2)
    else:
        logging.info('Flags differ in %s and %s', tab1, tab2)
    return res


def execute_dppp(args):
    command = ['DPPP'] + args
    logging.debug('executing %s', ','.join(command))
    dppp_process = Process(command)
    for i in range(_MAX_POOL):
        try:
            return_code = dppp_process.wait(_POOL_TIME)
            logging.debug('DPPP compression process %s finished with status: %s', dppp_process.pid, return_code)

            return return_code
        except TimeoutExpired as e:
            logging.debug('DPPP compression process %s still running', dppp_process.pid)
            continue


def check_return_code(return_code):
    if return_code > 0:
        logging.error('An error occurred in the DPPP execution: %s', return_code)
        raise SystemExit(return_code)
    else:
        pass


def split_ms(msin_path, startchan, nchan, msout_path=''):
    """
    use casacore.tables.msutil.msconcat() to concat the new MS files
    """
    if not msout_path:
        msout_path = msin_path.replace('.MS', f'_split_{startchan}_{nchan}.MS')
    logging.debug('Splitting file %s to %s', msin_path, msout_path)
    command_args = ['steps=[]',
                    'msout.overwrite=True',
                    f'msin={msin_path}',
                    f'msin.startchan={startchan}',
                    f'msin.startchan={nchan}',
                    f'msout={msout_path}']
    return_code = execute_dppp(command_args)
    logging.debug('Split of %s returned status code %s', msin_path, return_code)
    check_return_code(return_code)
    return msout_path


def compress(msin_path, msout_path='', bitrate=12):
    if not msout_path:
        msout_path = msin_path.replace('.MS', '_compressed.MS')
    logging.debug('Compressing file %s to %s', msin_path, msout_path)
    command_args = ['steps=[]',
                    'msout.storagemanager=dysco',
                    'msout.overwrite=True',
                    f'msin={msin_path}',
                    f'msout={msout_path}',
                    f'msout.storagemanager.databitrate={bitrate}']
    return_code = execute_dppp(command_args)
    logging.debug('Compression of %s returned status code %s', msin_path, return_code)
    check_return_code(return_code)
    return msout_path


def decompress(msin_path, msout_path=''):
    if not msout_path:
        msout_path = msin_path.replace('.MS', '_compressed.MS')
    logging.debug('Decompressing file %s to %s', msin_path, msout_path)
    command_args = ['steps=[]',
                    f'msin={msin_path}',
                    f'msout={msout_path}']
    return_code = execute_dppp(command_args)
    logging.debug('Decompression of %s returned status code %s', msin_path, return_code)
    check_return_code(return_code)
    return msout_path


def main():
    args = parse_args()
    setup_logging(args.verbose)
    if args.decompress:
        result = decompress(args.input, args.output)
    else:
        # msout1 = split_ms(args.input, 10464, 164, msout_path=args.input.replace('.MS', '_lower.MS')) # to verify with Tom
        msout2 = split_ms(args.input, 12288, 12288, msout_path=args.input.replace('.MS', '_upper.MS')) # upper half-band
        test_same_flags(msout2, args.flags)
        flagged_ms_path = apply_flags(msout2,
                                      flags_path=args.flags,
                                      msout_path=args.output)
        test_same_flags(flagged_ms_path, args.flags)
        result = compress(flagged_ms_path, args.output, bitrate=args.bitrate)
    return result


if __name__ =='__main__':
    main()