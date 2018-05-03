#  SCCS Keywords  "%Z% %M%  %I%  %H%"

# -*- coding: utf-8 -*-
"""
* This Price Determination Model and all associated code including without limitation,
* interface code was developed  by Boston Consulting Group  on behalf of UPS and is
* considered work for hire.
* The Pricing Determination Model and associated coded was provided to UPS without restriction.
* Any associated code including modifications of such code are deemed derivative work(s) of the
* Pricing Determination Model and ownership of such associated code vests with UPS.
* The use, disclosure, reproduction, modification, transfer, or transmittal of this work
* for any purpose in any form or by any means without the
* written permission of United Parcel Service is strictly prohibited.
* Confidential.  Unpublished Property of United Parcel Service.
* Use and Distribution limited solely to Authorized Persons.
*
*Copyright 2017 United Parcel Service of America, Inc.
* All Rights Reserved.
"""

STATIC_CONFIG = {
    "PATHS": {
        "OBJ":"./Object Files/",
        "INPUT":"./Input/",
         "VARIABLES":"./Model Variables/",
         "REPORTING_SUPPORT_FILES":"./Reporting Support Files/",
         "REPORTS":"./Reports/",
         "MODEL_OBJ_PATH":"/data/",
         "INPUT_PATH":"/data/inputs/",
         "support_path":"/bin/",
         "c2p_path":"/data/ipc/consumer2producer/",
         "p2c_path":"/data/ipc/producer2consumer/",
         "log_path":"/data/analytics/",
         "error_log_path":"/",
         "model_log_path":"/data/analytics/",
         "init_path":"/data/inputs/settings.csv",
         "HOME":"TP2_HOME",
         "LOG":"LOGDIR"
    },
    "DB": {
        'db_host': '.oracledb.ups.com'
    }
}

