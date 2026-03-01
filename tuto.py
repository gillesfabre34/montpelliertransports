import requests
from bs4 import BeautifulSoup

def tuto_python():
    start = "My name is"
    mylist = ["aaa", "bbb", "ccc"]

    print(f"{start} zzzzz")
    print(mylist)
    mylist[1] = "zzz"
    mylist.append("uuu")
    print(mylist)
    print(len(mylist))
    print("zzz" in mylist)
    print(mylist.count("aaa"))
    mylist.insert(2, "2222")
    print(mylist)
    mylist.pop(2)
    print(mylist)
    mylist.reverse()
    print(mylist)

    dict = {
        "name": "Logos",
        "age": 25
    }
    dict["city"] = "Montpellier"
    print(dict)
    del dict["age"]
    print(dict)
    print("city" in dict)

    #name = input("city:")
    if dict["city"] == "Palavas":
        print("Palavas")
    elif dict["city"] == "Carnon":
        print("Carnon")
    else:
        print("Montpellier")

    for x in range(5):
        print(x)

    website = requests.get("http://www.gillesfabre.com")
    print(website.content)
    soup = BeautifulSoup(website.content, "html.parser")
    titles = soup.findAll(["h1", "h2"])
    for title in titles:
        print(f"Title :{title}")

tuto_python()