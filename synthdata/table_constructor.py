import random
import string
import pandas as pd
from typing import List
from dataclasses import dataclass
from datetime import datetime, timedelta
from .table_configurator import ConfigConstructor


@dataclass
class TableConstructor(ConfigConstructor):

    def create_vtbfha(self) -> pd.DataFrame:
        data = []
        used_rfha, used_zuond, used_kontrh = set(), set(), set()
        for row in range(self.num_rows):
            for bukrs in self.bukrs_list:
                data.append({'bukrs': bukrs,
                             'rfha': self.generate_rfha(used_rfha),
                             'dcrdat_ha': self.get_random_date(),
                             'tcrtim_ha': self.get_random_time(),
                             'sgsart': random.choice(self.sgsart_list),
                             'sfhaart': str(random.choice(self.sfhaart_list)),
                             'zuond': self.generate_zuond(used_zuond),
                             'saktiv': int(random.choice(self.saktiv_list)),
                             'wgschft': random.choice(self.wgschft_list),
                             'kontrh': self.generate_kontrh(used_kontrh),
                             'rfhazul': random.choice(self.rfhazul_list),
                             })

        df = pd.DataFrame(data)
        df['dcrdat_ha'] = pd.to_datetime(df['dcrdat_ha'])
        df.drop_duplicates(subset=['rfha', 'bukrs'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'vtbfha.csv')
        return df

    def create_vtbfhapo(self, rfha_list: List[str]) -> pd.DataFrame:
        data = []
        for rfha in rfha_list:
            for bukrs in self.bukrs_list:
                for _ in range(random.randint(1, 10)):
                    dcrdat = self.get_random_date()
                    tcrtim = self.get_random_time()
                    dzterm = self.get_random_delta_date(dcrdat)
                    dbuchung = self.get_random_delta_date(dcrdat)
                    data.append({'bukrs': bukrs,
                                 'rfhazu': int(random.choice(self.rfhazul_list)),
                                 'rfha': rfha,
                                 'dcrdat': dcrdat,
                                 'gjahr': int(dcrdat.split('-')[0]),
                                 'tcrtim': tcrtim,
                                 'rfhazb': int(random.randint(1, 30)),
                                 'sfhazba': random.choice(self.sfhazba_list),
                                 'ssign': random.choice(['+', '-']),
                                 'dzterm': dzterm,
                                 'bzbetr': round(random.uniform(1.00000000e+04, 9.00000000e+08), 2),
                                 'dbuchung': dbuchung,
                                 'sbewebe': str(random.randint(1, 4))
                                 })

        df = pd.DataFrame(data)
        df['dcrdat'] = pd.to_datetime(df['dcrdat'])
        df.drop_duplicates(subset=['bukrs', 'rfhazu', 'rfha', 'dcrdat', 'tcrtim'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'vtbfhapo.csv')
        return df

    def create_vtbfhazu(self, rfha_list: List[str]) -> pd.DataFrame:
        data = []
        for rfha in rfha_list:
            for bukrs in self.bukrs_list:
                for num in range(random.randint(1, 15)):
                    data.append({'bukrs': bukrs,
                                 'rfhazu': int(num),
                                 'rfha': rfha,
                                 'dcrdat_zu': self.get_random_date(),
                                 'tcrtim_zu': self.get_random_time(),
                                 'nordext': f'Транш {random.randint(1, 10)}'
                                 })
        df = pd.DataFrame(data)
        df['dcrdat_zu'] = pd.to_datetime(df['dcrdat_zu'])
        df.drop_duplicates(subset=['bukrs', 'rfhazu', 'rfha'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'vtbfhazu.csv')
        return df

    def create_draw(self, zuond_list: List[str]) -> pd.DataFrame:
        data = []
        for zuond in zuond_list:
            data.append({'dokar': str(random.choice(['DFI', 'DFO'])),
                         'doknr': str(zuond),
                         'zz_num_reg': str(random.choice(self.zz_num_reg) + '/') +
                                       str(random.randint(100, 999)) + '-' +
                                       str((random.randint(2010, 2022))) + '-' +
                                       str(''.join(random.sample(string.ascii_uppercase, 3))),
                         'zz_hbkid': str(''.join(random.sample(string.ascii_uppercase, 3))) + str(0) +
                                     str(random.randint(0, 9)),
                         'zz_vvsart': str(random.choice(self.zz_vvsart)),
                         'zz_sfhaart': str(random.choice(self.zz_sfhaart_list)),
                         'zz_bankname': str(random.choice(self.zz_bankname)),
                         'zz_begda': self.get_random_date()
                         })

        df = pd.DataFrame(data)
        df['zz_begda'] = pd.to_datetime(df['zz_begda'])
        df.drop_duplicates(subset=['dokar', 'doknr'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'draw.csv')
        return df

    def create_t12(self, hbkid_list: List[str]) -> pd.DataFrame:
        data = []
        for hbkid in hbkid_list:
            for bukrs in self.bukrs_list:
                data.append({'bukrs': bukrs,
                             'hbkid': hbkid,
                             'banks': random.choice(['RU', 'ENG']),
                             'text1': f'Аккредитив счет {random.choice(self.codes_list)}',
                             'numsfh': f'4070{random.randint(20, 29)}40501701000{random.randint(300, 900)}',
                             'waers_t': str(random.choice(self.codes_list))
                             })

        df = pd.DataFrame(data)
        df.drop_duplicates(subset=['bukrs', 'hbkid'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 't12.csv')
        return df

    def create_vtb_asgn_limit(self, rfha_list: List[str]) -> pd.DataFrame:
        data = []
        for rfha in rfha_list:
            for bukrs in self.bukrs_list:
                for num in range(random.randint(1, 30)):
                    data.append({'relat_obj': str(bukrs) + '0000' + str(rfha),
                                 'limit_date': self.get_random_date(),
                                 'bukrs_relat_obj': str(bukrs),
                                 'rfha_relat_obj': str(rfha),
                                 'limit_currenc': random.choice(self.limit_currenc_list),
                                 'limit_pos_amount': round(random.uniform(1e6, 9e9), 2)
                                 })
        df = pd.DataFrame(data)
        df['limit_date'] = pd.to_datetime(df['limit_date'])
        df.drop_duplicates(subset=['relat_obj', 'limit_date', 'bukrs_relat_obj', 'rfha_relat_obj'],
                           inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'vtb_asgn_limit.csv')
        return df

    def create_tracv_accitem(self, rfha_list: List[str], dcrdat_list: List[datetime]) -> pd.DataFrame:
        data = []
        for rfha, dcrdat in zip(rfha_list, dcrdat_list):
            for bukrs in self.bukrs_list:
                for num in range(random.randint(1, 15)):
                    data.append({'bukrs': bukrs,
                                 'rfha': rfha,
                                 'acpostingdate': self.get_random_date(),
                                 'dis_flowtype': str(''.join(random.sample(string.ascii_uppercase, 3))) +
                                                 str(random.randint(100, 999)) + random.choice(['-', '+']),
                                 'posting_key': str(random.randint(0, 50)),
                                 'position_amt': random.uniform(-90000000, 90000000),
                                 'position_curr': random.choice(self.wgschft_list),
                                 'document_guid': f'00000000{random.randint(100000, 900000)}',
                                 'item_number': random.randint(0, 20),
                                 'os_guid_pi': f'00000000{random.randint(100000, 900000)}'
                                 })
        df = pd.DataFrame(data)
        df['acpostingdate'] = pd.to_datetime(df['acpostingdate'])
        df.drop_duplicates(subset=['document_guid', 'item_number', 'os_guid_pi'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'tracv_accitem.csv')
        return df

    def create_bseg(self, rfha_list: List[str], gjahr_list: List[int]) -> pd.DataFrame:
        data = []
        for rfha, gjahr in zip(rfha_list, gjahr_list):
            for bukrs in self.bukrs_list:
                for num in range(random.randint(1, 20)):
                    data.append({'bukrs': bukrs,
                                 'belnr': rfha,
                                 'gjahr': gjahr,
                                 'kblnr': str(random.randint(1000000000, 9999999999)),
                                 'buzei': int(random.randint(1, 15)),
                                 'wrbtr': random.uniform(100001, 10000999),
                                 })
        df = pd.DataFrame(data)
        df.drop_duplicates(subset=['bukrs', 'gjahr', 'kblnr', 'buzei'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'bseg.csv')
        return df

    def create_bkpf(self, rfha_list: List[str], gjahr_list: List[int]) -> pd.DataFrame:
        data = []
        for rfha, gjahr in zip(rfha_list, gjahr_list):
            for bukrs in self.bukrs_list:
                for num in range(random.randint(1, 3)):
                    data.append({'bukrs': bukrs,
                                 'belnr': rfha,
                                 'gjahr': gjahr,
                                 'waers_b': random.choice(self.waers_b_list),
                                 'awkey': f'00000000{random.randint(100000, 999999)}{str(gjahr)}',
                                 'xreversa': str(random.randint(0, 1)),
                                 })
        df = pd.DataFrame(data)
        df.drop_duplicates(subset=['bukrs', 'gjahr', 'belnr'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'bkpf.csv')
        return df

    def create_kblk(self, rfha_list: List[str], gjahr_list: List[int]) -> pd.DataFrame:
        data = []
        for rfha, gjahr in zip(rfha_list, gjahr_list):
            for num in range(random.randint(1, 50)):
                data.append({'belnr': rfha,
                             'kerdat': self.get_random_date(),
                             'gjahr': gjahr,
                             'zz_bezakc': random.choice([None, 'X']),
                             'zz_pyord_r': f'8{random.randint(60000, 69999)}',
                             'zz_zh2h': random.choice([None, 'X']),
                             'zz_vblnr': str(random.randint(300, 4000)),
                             'zz_bukrs': random.choice(self.bukrs_list),
                             'zz_hbkid': f'100000{random.randint(100, 999)}'
                             })
        df = pd.DataFrame(data)
        df['kerdat'] = pd.to_datetime(df['kerdat'])
        df.drop_duplicates(subset=['belnr'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'kblk.csv')
        return df

    def create_pyordp(self, rfha_list: List[str], gjahr_list: List[int]) -> pd.DataFrame:
        data = []
        for rfha, gjahr in zip(rfha_list, gjahr_list):
            pyord = str(random.randint(1000000, 9999999999))
            for bukrs in self.bukrs_list:
                for num in range(random.randint(1, 5)):
                    data.append({'pyord': pyord,
                                 'bukrs': bukrs,
                                 'belnr': rfha,
                                 'gjahr': gjahr,
                                 'buzei': random.randint(0, 30),
                                 })
        df = pd.DataFrame(data)
        df.drop_duplicates(subset=['pyord', 'gjahr', 'bukrs', 'belnr', 'buzei'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'pyordp.csv')
        return df

    def create_pyordh(self, pyord_list: List[str], gjahr_list: List[int]) -> pd.DataFrame:
        data = []
        for pyord, gjahr in zip(pyord_list, gjahr_list):
            for num in range(random.randint(1, 10)):
                data.append({'pyord': pyord,
                             'laufd': self.get_random_date(),
                             'gjahr': gjahr,
                             'waers': random.choice(self.waers_list),
                             'zbukr': random.choice(self.bukrs_list),
                             'hbkid': str((''.join(random.sample(string.ascii_uppercase, 4)))) +
                                      str(random.randint(0, 9)),
                             'rbetr': random.uniform(-90000000, 90000000)
                             })
        df = pd.DataFrame(data)
        df['laufd'] = pd.to_datetime(df['laufd'])
        df.drop_duplicates(subset=['pyord'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'pyordh.csv')
        return df

    def create_kblp(self, rfha_list: List[str]) -> pd.DataFrame:
        data = []
        for rfha in (rfha_list):
            for num in range(random.randint(1, 3)):
                data.append({'belnr': rfha,
                             'blpos': int(random.choice([2, 3, 1, 4, 5])),
                             'zz_rfha': '100000' + str(random.randint(1000, 9999)),
                             'zz_rfhazu': int(random.choice(self.rfhazu_list)),
                             'zz_rfhazb': int(random.randint(1, 31)),
                             'zz_dcrdat': self.get_random_date(),
                             'zz_tcrtim': self.get_random_time(),
                             'wtges': random.uniform(100000, 90000000)
                             })
        df = pd.DataFrame(data)
        df['zz_dcrdat'] = pd.to_datetime(df['zz_dcrdat'])
        df.drop_duplicates(subset=['belnr', 'blpos'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'kblp.csv')
        return df

    def create_but000(self, kontrh_list: List[str]) -> pd.DataFrame:
        data = []
        for kontrh in kontrh_list:
            data.append({'partner': kontrh,
                         'name_org1': 'Cтрока 1',
                         'name_org2': 'Cтрока 2',
                         'name_org3': 'Cтрока 3',
                         'name_org4': 'Cтрока 4',
                         })
        df = pd.DataFrame(data).drop_duplicates(subset=['partner'])
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 'but000.csv')
        return df

    def create_t001(self) -> pd.DataFrame:
        data = []
        for bukr in range(100, 9999):
            data.append({'bukrs': str(bukr),
                         'butxt': str(random.choice(['ПАО', 'ООО', 'АО', 'НН', 'НОУ'])) +
                                  ' ' + str(random.choice(self.butxt_list))
                         })
        df = pd.DataFrame(data).drop_duplicates(subset=['bukrs'])
        df.reset_index(drop=True, inplace=True)
        self.save_dataframe_to_csv(df, 't001.csv')
        return df
