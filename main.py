#!/usr/bin/env python

""" Python script to check and detect potential errors on Serious Adverse Events
(SAEs) in the ICARIA Clinical Trial, specifically on SAE numeration and possible
gohst entry errors """

__author__ = "Andreu Bofill"
__copyright__ = "Copyright 2024, ISGlobal Maternal, Child and Reproductive Health"
__credits__ = ["Andreu Bofill"]
__license__ = "MIT"
__version__ = "0.0.1"
__date__ = "20240715"
__maintainer__ = "Andreu Bofill"
__email__ = "andreu.bofill@isglobal.org"
__status__ = "Finished"

import sae
from datetime import datetime

if __name__ == '__main__':
    sae.get_SAE_events()
    print("\n[{}] FINISHED".format(datetime.now()))