"""
Useful functions for LCA-related projects that aren't really re-usable



"""

GABI_URLS = {"GaBi2014": 'http://www.gabi-software.com/support/gabi/gabi-database-2014-lci-documentation/professional-database-2014/',
             "Lean2014": 'http://www.gabi-software.com/support/gabi/gabi-5-lci-documentation/data-sets-by-database-modules/lean-database/'}

PROFESSIONAL = 'GaBi2014'
LEAN = 'Lean2014'


def gabi_package_list(url):
    """
    Generates a list of data frames containing process lists from the Thinkstep GaBi Professional databases

    :param url: URL for a page containing the processListTable(s)
    :return: a list of dataframe objects containing the contents of the processListTable(s)
    """

    import pandas as pd
    from urllib2 import urlopen
    from BeautifulSoup import BeautifulSoup
    html = urlopen(url).read()
    dom = BeautifulSoup(html)
    t = dom.findAll('table', attrs={'id': 'processListTable'})

    print 'Found %d processListTables' % len(t)

    # create data frame containing list headings
    p = []
    for table in t:
        print 'Processing '
        columns = [k.text for k in table.findAll('th')]
        data = []
        for row in table.findAll('tr'):
            entries = [k.text for k in row.findAll('td')]
            if len(entries) != 0:
                data.append(entries)

        if len(columns) == 0:
            columns = ['Column' + str(i+1) for i in range(0, len(data[0]))]

        print 'Adding DataFrame with %d columns and %d rows' % (len(columns), len(data))
        df = pd.DataFrame(columns=columns, data=data)

        p.append(df)

    return p

# example usage:
# >>> P = gabi_package_list(GABI_URLS[PROFESSIONAL])

