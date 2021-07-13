import psycopg2
import re

from wbtools.db.abstract_manager import AbstractWBDBManager


class WBGeneDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)

    def get_gene_name_from_id(self, gene_id: str):
        gene_id = gene_id.lstrip("WBGene")
        locus = self._get_single_field(gene_id, "gin_locus")
        if not locus:
            locus = self._get_single_field(gene_id, "gin_seqname")
        if not locus:
            raise Exception("Gene name not found")
        return locus

    def get_all_gene_names(self):
        with self.get_cursor() as curs:
            curs.execute("SELECT joinkey, gin_locus from gin_locus")
            res = curs.fetchall()
            gene_locus = {row[0]: row[1] for row in res}
        with self.get_cursor() as curs:
            curs.execute("SELECT joinkey, gin_seqname from gin_seqname")
            res = curs.fetchall()
            gene_seqname = {row[0]: row[1] for row in res}
        with self.get_cursor() as curs:
            curs.execute("SELECT joinkey, gin_dead from gin_dead")
            res = curs.fetchall()
            gene_dead = {row[0]: re.findall(row[1], "WBGene[0-9]+") for row in res}
        gene_ids = sorted(list(set(gene_locus.keys()) | set(gene_seqname.keys()) | set(gene_dead.keys())))
        return {gene_id: [gene_locus[gene_id]] if gene_id in gene_locus else [gene_seqname[gene_id]] if gene_id in gene_seqname else gene_dead[gene_id] for
                gene_id in gene_ids}

    def get_weighted_gene_interactions(self):
        with self.get_cursor() as curs:
            curs.execute("select least(int_genebait.int_genebait, translate(int_genetarget.int_genetarget,'\"','')), "
                         "greatest(int_genebait.int_genebait, translate(int_genetarget.int_genetarget,'\"','')), "
                         "count(*) from int_genebait JOIN int_genetarget "
                         "ON int_genebait.joinkey = int_genetarget.joinkey "
                         "group by least(int_genebait.int_genebait, "
                         "translate(int_genetarget.int_genetarget, '\"', '')), "
                         "greatest(int_genebait.int_genebait, translate(int_genetarget.int_genetarget, '\"', ''))")
            res = curs.fetchall()
            return [(row[0], row[1], row[2]) for row in res] if res else []

