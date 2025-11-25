from .main import PXMain

def go():
    print('-- start --')
    px_main = PXMain()
    px_main.run()
    print('-- stop  --')

if __name__ == "__main__":
    go()
