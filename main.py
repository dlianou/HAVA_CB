from ClickBank_HAVA.goldvida_refunds import get_gv_refunds
from ClickBank_HAVA.goldvida_chargebacks import get_gv_chbks
from time import sleep

# Define a fxn called main
def main():
    get_gv_refunds()
    sleep(10)
    #get_gv_chbks()



# this will help me control the order of the execution
if __name__ == '__main__':
    main()