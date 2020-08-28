# This is a sample Python script.
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from decimal import Decimal
from scipy.stats import ksone
import os.path


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    print('On Git Hub')
    print('change on github')

def connect_redshift_db():
    cnx = psycopg2.connect(dbname='dwh',
                           host='redshift-prod.data.service.home24.net',
                           port='5439',
                           password='zQZ8x6ZhRoTf7GS',
                           user='gabriele_sabato')
    return cnx


def query_to_dataframe(_cursor, _query):
    _cursor.execute(_query)
    colnames = [desc[0] for desc in _cursor.description]
    print(colnames)
    return pd.DataFrame(_cursor.fetchall(), columns=colnames)


### QUERIES
query_comp_price = """
WITH Papi AS (
    WITH smallpapi AS (
        SELECT CAST(papi.gtin as bigint) as int_gtin,
               papi.shop_domain,
               papi.price_with_shipping as comp_price,
               papi.country_code,
               to_number(to_char(papi.meta_scrape_date, 'yyyymmddhhmiss'), '99999999999999') as wscrape_date
        FROM pricing_sandbox.priceapi_offer as papi
        WHERE gtin is not null and gtin ~'^[0-9]+$' and wscrape_date LIKE '202007%' and shop_domain is not null and comp_price is not null and country_code = 'DE'
    ),
         ranked_webscrape AS (
             SELECT smallpapi.*, ROW_NUMBER() OVER (PARTITION BY int_gtin ORDER BY smallpapi.wscrape_date DESC) as rw
             FROM smallpapi
         )
    SELECT int_gtin, wscrape_date, shop_domain, country_code, comp_price
    FROM ranked_webscrape
    WHERE rw = 1
),item_w_price_date AS (
    SELECT CAST(sadi.item_gtin as bigint) as int_gtin ,
           sadi.item_skey,
           sadi.item_code,
           sadi.item_main_category,
           sadi.item_sub_category_1,
           sadi.item_supplier_regular,
           fiph.item_price_final as home24_price,
           dcal.date_skey as item_date
    FROM star_analytical.d_item AS sadi
    INNER JOIN star_analytical.f_item_prices_history as fiph on fiph.item_skey = sadi.item_skey
    INNER JOIN star_analytical.d_calendar as dcal on dcal.date_skey = fiph.update_date_skey
    WHERE sadi.item_gtin is not null and sadi.item_gtin ~ '^[0-9]+$' and dcal.year_month='2020-07' and sadi.item_gtin_comparable = 'comparable'
), item_dist_w_price AS (
    SELECT *
    FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY int_gtin ORDER BY item_date DESC) AS rdate
        FROM item_w_price_date
        ) as ranked
    WHERE ranked.rdate = 1
), final as (
SELECT DISTINCT shop_domain, Papi.int_gtin as gtin, wscrape_date, item_date, item_main_category, home24_price, comp_price,
       CASE
           WHEN Papi.comp_price <> 0 THEN home24_price/Papi.comp_price ELSE NULL END  mpi_price
FROM Papi
INNER JOIN item_dist_w_price on item_dist_w_price.int_gtin = Papi.int_gtin
WHERE mpi_price < 5.0 and mpi_price is not null
), final_count_top_shops AS(
SELECT TOP 21 DISTINCT shop_domain, COUNT(shop_domain) as N
FROM final
GROUP BY 1
HAVING N > 100
ORDER BY N DESC
) SELECT *
FROM final
WHERE shop_domain in(SELECT shop_domain FROM final_count_top_shops)"""

query_full_catalog = """WITH Papi AS (
    SELECT *,
           ROW_NUMBER()
           OVER (PARTITION BY CAST(papi.gtin as bigint), coalesce(shop_domain, shop_name) ORDER BY CAST(papi.meta_scrape_date as DATE) DESC) as rw
    FROM pricing_sandbox.priceapi_offer as papi
    WHERE papi.gtin is not null and papi.gtin ~'^[0-9]+$' and country_code ='DE'
), PPapi AS (
        SELECT CAST(Papi.gtin as bigint) as int_gtin,
               Papi.shop_domain,
               Papi.price_with_shipping as comp_price,
               Papi.country_code,
               Papi.shop_name,
               CAST(Papi.meta_scrape_date as DATE) as wscrape_date,
               coalesce(shop_domain, shop_name) as shop_final_name
        FROM Papi
        WHERE Papi.gtin is not null and Papi.gtin ~'^[0-9]+$' and Papi.country_code = 'DE' and rw = 1

), item_price_history_latest as (
    SELECT item_skey, item_price_final, update_date_skey, ROW_NUMBER() over (PARTITION BY item_skey ORDER BY update_date_skey DESC) as rfiph
    FROM star_analytical.f_item_prices_history
)

,item_w_price_date AS (
    SELECT CAST(sadi.item_gtin as bigint) as int_gtin,
           sadi.item_skey,
           sadi.item_code,
           /*
           sadi.item_main_category,
           sadi.item_sub_category_1,
           sadi.item_supplier_regular,*/
           item_price_history_latest.item_price_final as home24_price,
           item_price_history_latest.update_date_skey as item_date,
           PPapi.comp_price,
           PPapi.shop_final_name,
           PPapi.int_gtin as gtin,
           PPapi.wscrape_date,
           CASE WHEN PPapi.comp_price <> 0 THEN home24_price/PPapi.comp_price END  mpi_price
    FROM star_analytical.d_item AS sadi
    INNER JOIN item_price_history_latest on item_price_history_latest.item_skey = sadi.item_skey
        /* I need to select only the last update for the same item_skey */
    INNER JOIN PPapi on  PPapi.int_gtin = CAST(sadi.item_gtin as bigint)
    WHERE sadi.item_gtin is not null and sadi.item_gtin ~ '^[0-9]+$' and item_price_history_latest.rfiph = 1
      and home24_price > 0. and mpi_price > 0. and mpi_price < 5.0
),final_count_top_shops AS (
    SELECT TOP 30 DISTINCT shop_final_name, COUNT(shop_final_name) as N
    FROM item_w_price_date
    GROUP BY 1
    HAVING N > 100
    ORDER BY N DESC
)SELECT  gtin, item_skey, item_code, wscrape_date, item_date, shop_final_name, home24_price, comp_price, mpi_price
FROM item_w_price_date
WHERE shop_final_name in(SELECT shop_final_name FROM final_count_top_shops)"""


def normality_test(data, quantity, shop_domain='OTTO', log_test='no_log', dir=''):
    if log_test == 'log':
        data_log = log_df(data, quantity)
    else:
        data_log = data
    data_home24 = shop_df(data_log, shop_domain)
    if log_test == 'log':
        quantity = log_test + '_' + quantity
    mpi_median = data_home24[quantity].astype(float).median()
    mpi_mean = data_home24[quantity].astype(float).mean()
    mpi_std = data_home24[quantity].astype(float).std()
    mpi_mad = data_home24[quantity].astype(float).mad()
    mpi_mean_err = mpi_std / np.sqrt(len(data_home24[quantity]))
    mpi_mad_err = mpi_mad / np.sqrt(len(data_home24[quantity]))
    mpi_median_err = 1.2533 * mpi_mean_err
    data_array = data_home24[quantity].astype(float).to_numpy()
    loc, scale = stats.norm.fit(data_array)
    # create a normal distribution with loc and scale
    n = stats.norm(loc=loc, scale=scale)
    KS_D, p = stats.kstest(data_array, n.cdf)
    ks_D_crit = ks_critical_value(len(data_home24[quantity]))
    DAP_K2, DAP_p = stats.normaltest(data_array)
    info = f'Shop name  = {shop_domain} \n\n' \
           f'# of items    = {len(data_home24[quantity])} \n' \
           f'MPI $\mu$            = {round(mpi_mean, 3)} ± {round(mpi_mean_err, 3)} \n' \
           f'MPI median  = {round(mpi_median, 3)} ± {round(mpi_median_err, 3)}\n' \
           f'MPI $\sigma$            = {round(mpi_std, 3)}\n' \
           f'MPI MAD       = {round(mpi_mad, 3)}\n' \
           f'MPI MAD err  = {round(mpi_mad_err, 3)}\n' \
           f'KS-D             = {round(KS_D, 2)}\n' \
           f'KS-p              = {Decimal(p):.2E}\n' \
           f'DAP_K2         = {round(DAP_K2)}\n' \
           f'DAP_p           = {Decimal(DAP_p):.2E}' \
        # f'KS-D_crit        = {round(ks_D_crit, 3)}\n' \

    # f'K_square        = {round(Ksquare,3)}'
    fig = plt.figure()
    title = 'Normality test'
    if log_test == 'log':
        title = 'Log-' + title
    fig.suptitle(title, fontsize=14, fontweight='bold')
    ax = fig.add_subplot(111)
    ax.set_xlabel('MPI')
    ax.set_ylabel('Number of normalized items')
    ax.text(0.6, 0.7, info, horizontalalignment='left', verticalalignment='center', transform=ax.transAxes, fontsize=10)

    plt.hist(data_array, bins=np.arange(data_array.min(), data_array.max(), 0.05), rwidth=1., density=True)
    x = np.arange(data_array.min(), data_array.max(), 0.001)
    # print(len(x))
    plt.plot(x, n.pdf(x), 'r-')
    plt.savefig(f'{dir}/Normality_test_{shop_domain}_{log_test}.png')
    plt.show()

    # print('KS-statistic D = %6.3f pvalue = %6.4f' %
    """ alpha = 1e-3
    if p < alpha:  # null hypothesis: x comes from a normal distribution
        print("The null hypothesis can be rejected")
    else:
        print("The null hypothesis cannot be rejected")
    """


def beta_test(data, quantity, shop_domain):
    data_home24 = data[data['shop_final_name'] == shop_domain]
    data_array = data_home24[quantity].astype(float).to_numpy()
    data_arr_log = np.log(data_array)
    alpha, beta, loc, scale = stats.beta.fit(data_array)
    # create a normal distribution with loc and scale
    b = stats.beta(alpha, beta, loc=loc, scale=scale)
    KS_D, p = stats.kstest(data_array, b.cdf)
    ks_D_crit = ks_critical_value(len(data_home24['mpi_price']))
    print(KS_D, ks_D_crit)


def plot_quantity(data, quantity, shop_domain='home24.de'):
    title = f'{quantity} histogram  '
    # ax = data[['Returns', 'Strategy']].cumsum().apply(np.exp).plot(figsize = (10, 6))
    data_home24 = data[data['shop_final_name'] == shop_domain]
    # ax.get_legend().set_bbox_to_anchor((0.25, 0.85))
    ax = data_home24[quantity].astype(float).plot(kind='hist', bins=[i / 10. for i in range(0, 30)])
    plt.title(title)
    # plt.savefig(f'Return_vs_strat_{asset_name}_MDt_interval_{factor}.png')
    plt.show()
    # plt.clf()


def ks_critical_value(n_trials, alpha=0.05):
    return ksone.ppf(1 - alpha / 2, n_trials)


def scipy_normtest_ex():
    pts = 15000
    np.random.seed(28041990)
    a = np.random.normal(0, 1, size=pts)
    b = np.random.normal(2, 1, size=pts)
    x = np.concatenate((a, b))
    k2, p = stats.normaltest(x)
    alpha = 1e-3
    loc, scale = stats.norm.fit(x)
    # create a normal distribution with loc and scale
    n = stats.norm(loc=loc, scale=scale)
    KS_D, KS_p = stats.kstest(x, n.cdf)
    KS_crit = ks_critical_value(100)
    info = f'k2 = {k2}\np = {p}\nKS-D = {KS_D}\nKS_p = {KS_p}\nKS_crit = {KS_crit}'
    print(info)


def shapiro_test_ext(array):
    # np.random.seed(12345678)
    # x = stats.norm.rvs(loc=5, scale=3, size=100)
    shapiro_test = stats.shapiro(array)
    test_stat_shapiro = shapiro_test.statistic
    p_v = shapiro_test.pvalue
    alpha = 0.05
    if p_v > alpha:
        print("The distribution is normal")
    print(test_stat_shapiro, p_v)


def QQ_test(x, shop_name, log, dir):
    plt.figure()
    ax = plt.subplot(111)
    # x = stats.norm.rvs(loc=0, scale=1, size=nsample)
    # for distr in [stats.loggamma, stats.logistic, stats.lognorm ]:
    # res = stats.probplot(x,dist=stats.lognorm, sparams=(0.8,), plot=plt)
    res = stats.probplot(x, sparams=(1.,), plot=plt)
    # print(res)
    plt.title(f'QQ_plot \n{shop_name}_{log}')
    plt.savefig(f'{dir}/QQ_plot_{shop_name}_{log}.png')
    # plt.show()


def QQ_cauchy_test(x, shop_name, log):
    plt.figure()
    ax = plt.subplot(111)
    # x = stats.norm.rvs(loc=0, scale=1, size=nsample)
    # for distr in [stats.loggamma, stats.logistic, stats.lognorm ]:
    # res = stats.probplot(x,dist=stats.lognorm, sparams=(0.8,), plot=plt)
    res = stats.probplot(x, dist=stats.cauchy, plot=plt)
    plt.title(f'QQ_plot \n{shop_name}_{log}')
    plt.show()
    # plt.savefig(f'QQ_no_out/QQ_plot_cauchy_{shop_name}_{log}.png')


def obtain_mpi_price_array(data, quantity='mpi_price', shop_domain='home24.de', flag='no_log'):
    if flag == 'no_log':
        data_home24 = data[data['shop_final_name'] == shop_domain]  # select the shop
        return data_home24[quantity].astype(float).to_numpy()
    else:
        data['log_' + quantity] = np.log(data[quantity].astype(float))  # obtain the log
        data_home24 = data[data['shop_final_name'] == shop_domain]  # select the shop
        return data_home24['log_' + quantity].astype(float).to_numpy()


def log_df(data, quantity='mpi_price'):
    data['log_' + quantity] = np.log(data[quantity].astype(float))
    return data


def shop_df(data, shop_domain='home24.de'):
    data_shop = data[data['shop_final_name'] == shop_domain]  # select the shop
    return data_shop


def run_program(query, dir, flag):
    #    comp_price_df = pd.DataFrame()
    if not os.path.isfile('data/full_catalog.csv') or flag is True:
        print('Data file not found or needed to be updated, requesting query from the db')
        comp_price_df = query_to_dataframe(cursor_redshift, query)
        comp_price_df.to_csv(r'data/full_catalog.csv', index=False)

    else:
        print('File exists and the query is updated')
        comp_price_df = pd.read_csv('data/full_catalog.csv')

    print(len(set(comp_price_df['shop_final_name'])))
    for shop_name in set(comp_price_df['shop_final_name'].to_numpy()):
        for log in ['no_log','log']:
            normality_test(comp_price_df, 'mpi_price', shop_name, log, dir)
            QQ_test(obtain_mpi_price_array(comp_price_df, 'mpi_price', shop_name, log), shop_name, log, dir)


if __name__ == '__main__':
    cnx_redshift = connect_redshift_db()
    cursor_redshift = cnx_redshift.cursor()
    run_program(query_full_catalog, 'full_catalog', flag=False)

    # print(comp_price_df.head())
    # beta_test(comp_price_df,'mpi_price','OTTO')
    # plot_quantity(comp_price_df,'mpi_price','home24.de')
    # QQ-plot
    # scipy_normtest_ex()
    # shapiro_test_ext(obtain_mpi_price_array(comp_price_df))
    # QQ_cauchy_test(obtain_mpi_price_array(comp_price_df,'mpi_price','ebay.com','log'),'ebay.com','log')
    # QQ_test(obtain_mpi_price_array(comp_price_df, 'mpi_price', 'home24.de', 'no_log'))
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
