package Support;

import Support.Secundario;
import nodes.SlaveIdentifier;

public class RoundRobin{
    public SlaveIdentifier si;
    public int atual;
    public int ativos;

    public RoundRobin(SlaveIdentifier si) {
        this.si = si;
        this.atual = 0;
        ativos = 0;
        if(this.si.ativo){
            ativos++;
        }
        for(Secundario s: this.si.secundarios.values()){
            if(s.ativo){
                ativos++;
            }
        }
    }

    public String proximo(){
        if(atual == 0){
            atual = (atual+1) % ativos;
            return si.primario();
        }
        else{
            int quantos = 0;

            for(Secundario s: this.si.secundarios.values()){
                if(s.ativo){
                    quantos++;
                    if(quantos == atual){
                        atual = (atual+1) % ativos;
                        return s.endereco;
                    }
                }
            }
        }
        return si.primario();
    }
}
