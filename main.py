#!/usr/bin/env python
""" Python script to manage different components of the reporting of Serious Adverse Events (SAEs) in the ICARIA
Clinical Trial. These components are: (1) SAE numbering, etc."""

__author__ = "Andreu Bofill"
__copyright__ = "Copyright 2024, ISGlobal Maternal, Child and Reproductive Health"
__credits__ = ["Andreu Bofill"]
__license__ = "MIT"
__version__ = "0.0.1"
__date__ = "20240715"
__maintainer__ = "Andreu Bofill"
__email__ = "andreu.bofill@isglobal.org"
__status__ = "Dev"


import tokens, sae

import redcap

if __name__ == '__main__':
    sae.get_SAE_events()

    #sae.get_files()

#    sae.info_sae()