import argparse
from synthdata import DataGenerator


def generate_data(tb_name: str = 'all'):
    dg = DataGenerator()

    if tb_name == 'all':
        dg.table_creating_vtbfha()
        dg.table_creating_vtbfhapo()
        dg.table_creating_draw()
        dg.table_creating_kblp()
        dg.table_creating_vtbfhazu()
        dg.table_creating_but000()
        dg.table_creating_vtb_asgn_limit()
        dg.table_creating_t12()
        dg.table_creating_bseg()
        dg.table_creating_bkpf()
        dg.table_creating_kblk()
        dg.table_creating_pyordp()
        dg.table_creating_tracv_accitem()
        dg.table_creating_pyordh()

    if tb_name == 'vtbfha':
        dg.table_creating_vtbfha()

    if tb_name == 'vtbfhapo':
        dg.table_creating_vtbfhapo()

    if tb_name == 'draw':
        dg.table_creating_draw()

    if tb_name == 'kblp':
        dg.table_creating_kblp()

    if tb_name == 'vtbfhazu':
        dg.table_creating_vtbfhazu()

    if tb_name == 'but000':
        dg.table_creating_but000()

    if tb_name == 'vtb_asgn_limit':
        dg.table_creating_vtb_asgn_limit()

    if tb_name == 't12':
        dg.table_creating_t12()

    if tb_name == 'bseg':
        dg.table_creating_bseg()

    if tb_name == 'bkpf':
        dg.table_creating_bkpf()

    if tb_name == 'kblk':
        dg.table_creating_kblk()

    if tb_name == 'pyordp':
        dg.table_creating_pyordp()

    if tb_name == 'tracv_accitem':
        dg.table_creating_tracv_accitem()

    if tb_name == 'pyordh':
        dg.table_creating_pyordh()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generation of synthetic values')
    parser.add_argument('table', type=str, help='Table name or use "all" for all tables', default='all',
                        choices=['bseg', 'bkpf', 'kblk', 'pyordp', 'tracv_accitem', 'kblp', 'draw', 't001', 't12'
                                 'vtbfha', 'vtbfhapo', 'vtbfhazu', 'but000', 'vtb_asgn_limit', 'pyordh', 'all'])

    args = parser.parse_args()
    generate_data(tb_name=args.table)



