import os
import pandas as pd
from typing import List, Tuple, NoReturn
from .table_constructor import TableConstructor


class DataGenerator(TableConstructor):
    def __init__(self):
        super().__init__()
        self.rfha_list = self.table_creating_vtbfha()
        self.rfha_ls, self.gjahr_ls, self.dcrdat_ls = self.table_creating_vtbfhapo()
        self.zz_hbkid_ls = self.table_creating_draw()
        self.pyord_ls, self.pyord_ls_gjahr = self.table_creating_pyordp()

    def table_creating_vtbfha(self) -> List[str]:
        if not os.path.exists(os.path.join(os.getcwd(), 'data')):
            os.makedirs(os.path.join(os.getcwd(), 'data'))
            vtbfha = self.create_vtbfha()
        elif not os.path.exists('data/vtbfha.csv'):
            vtbfha = self.create_vtbfha()
        else:
            vtbfha = pd.read_csv('data/vtbfha.csv', sep=';')

        return list(vtbfha['rfha'].unique())

    def table_creating_vtbfhapo(self) -> Tuple[List[str], List[str], List[str]]:
        vtbfhapo = None
        if not os.path.exists('data/vtbfhapo.csv'):
            self.table_creating_vtbfha()
            self.create_vtbfhapo(self.rfha_list)
            vtbfhapo = pd.read_csv('data/vtbfhapo.csv', sep=';')
        else:
            vtbfhapo = pd.read_csv('data/vtbfhapo.csv', sep=';')

        return list(vtbfhapo['rfha']), list(vtbfhapo['gjahr']), list(vtbfhapo['dcrdat'])

    def table_creating_draw(self) -> List[str]:
        draw = None
        if not os.path.exists('data/draw.csv'):
            self.table_creating_vtbfhapo()
            self.create_draw(self.rfha_list)
            draw = pd.read_csv('data/draw.csv', sep=';')
        else:
            draw = pd.read_csv('data/draw.csv', sep=';')

        return list(draw['zz_hbkid'].unique())

    def table_creating_pyordp(self) -> Tuple[List[str], List[str]]:
        pyordp = None
        if not os.path.exists('data/pyordp.csv'):
            self.table_creating_vtbfhapo()
            self.create_pyordp(self.rfha_ls, self.gjahr_ls)
            pyordp = pd.read_csv('data/pyordp.csv', sep=';')
        else:
            pyordp = pd.read_csv('data/pyordp.csv', sep=';')

        return list(pyordp['pyord']) ,list(pyordp['gjahr'])

    def table_creating_t12(self) -> NoReturn:
        if not os.path.exists('data/t12.csv'):
            self.table_creating_draw()
            self.create_t12(self.zz_hbkid_ls)

    def table_creating_t001(self) -> NoReturn:
        self.create_t001()

    def table_creating_kblp(self) -> NoReturn:
        self.create_kblp(self.rfha_list)

    def table_creating_vtbfhazu(self) -> NoReturn:
        self.create_vtbfhazu(self.rfha_list)

    def table_creating_but000(self) -> NoReturn:
        self.create_but000(self.rfha_list)

    def table_creating_vtb_asgn_limit(self) -> NoReturn:
        self.create_vtb_asgn_limit(self.rfha_list)

    def table_creating_bseg(self) -> NoReturn:
        self.create_bseg(self.rfha_ls, self.gjahr_ls)

    def table_creating_bkpf(self) -> NoReturn:
        self.create_bkpf(self.rfha_ls, self.gjahr_ls)

    def table_creating_kblk(self) -> NoReturn:
        self.create_kblk(self.rfha_ls, self.gjahr_ls)

    def table_creating_tracv_accitem(self) -> NoReturn:
        self.create_tracv_accitem(self.rfha_ls, self.dcrdat_ls)

    def table_creating_pyordh(self) -> NoReturn:
        self.table_creating_pyordp()
        self.create_pyordh(self.pyord_ls, self.pyord_ls_gjahr)

