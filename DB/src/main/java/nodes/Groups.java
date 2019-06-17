package nodes;

import spread.SpreadException;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Scanner;

public class Groups {
    public static Scanner s = new Scanner(System.in);


    public HashMap<String, Grupo> grupos = new HashMap<>();

    public Groups(){}

    public void adicionaConexao(String id, String grupo) throws SpreadException, UnknownHostException {
        System.out.println("ATENCAO QUE O GRUPO ESTA NULO!!!");
        Grupo c = null;// new Grupo(id,grupo);
        grupos.put(grupo,c);
    }

    public void desconecta(String grupo) throws SpreadException{
        Grupo c = grupos.remove(grupo);
        if(c != null){
            c.desconectar();
        }
    }

    public void sair() throws SpreadException {
        for(Grupo c: grupos.values()){
            c.desconectar();
        }
    }

    public String toString(){
        for(Grupo c: grupos.values()){
            System.out.println(c);
        }
        return grupos.toString();
    }


    private static String leString(String msg){
        String emp = null;
        do {
            try {
                emp = s.nextLine();

            }catch(Exception e){
                System.out.println(msg);
            }
        }while(emp == null);

        return emp;
    }

    private static int leInteiro(String msg){
        int quant = -1;
        do {
            System.out.println("Opção:");
            String aux = s.nextLine();

            try {
                quant = Integer.parseInt(aux);
            }
            catch(Exception e){
                System.out.println(msg);
            }
        }while(quant < 0);
        return quant;
    }

    public static void main(String[] args) throws SpreadException, UnknownHostException {
        Groups g = new Groups();

        boolean continua = true;
        while(continua){

            System.out.println("0 - Adicionar grupo");
            System.out.println("1 - Desconectar");
            System.out.println("2 - Ver grupos");
            System.out.println("Outro - Sair");

            int aux = leInteiro("Introduza um valor válido (>=0)");
            System.out.println("Op: " + aux);

            switch (aux){
                case 0:
                    System.out.println("Indique o grupo ao qual se quer juntar:");
                    String grupo = leString("Coloque uma string válida!");
                    System.out.println("Indique o id no grupo ao qual se quer juntar:");
                    String id = leString("Coloque um id válido!");

                    g.
                            adicionaConexao(id,grupo);
                    break;
                case 1:
                    System.out.println("Indique o grupo ao qual se quer desconectar:");
                    String grupoD = leString("Coloque uma string válida!");
                    g.desconecta(grupoD);
                    break;

                case 2:
                    g.toString();
                    break;
                default:
                    g.sair();
                    continua = false;
                    break;

            }

        }

    }
}
