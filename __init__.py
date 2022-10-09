from L1C_Conversion_Pipeline.l1c_file import L1CFile


if __name__ == '__main__':
    l1c = L1CFile()
    year_str = '2008'
    date_str = '2008_03_23'

    # Grasp Extraction at this level

    l1c.run_group_of_caltrak_to_l1c(year_str, date_str)
