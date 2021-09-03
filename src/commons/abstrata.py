from abc import ABC, abstractmethod


class Abstrata(ABC):

    def __init__(self, fr_path_al,fr_path_disc,to_path):
        self.fr_path_al = fr_path_al
        self.fr_path_disc = fr_path_disc
        self.to_path = to_path
    @abstractmethod
    def processa():
        return 1
