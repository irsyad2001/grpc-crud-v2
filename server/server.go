// server/server.go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"grpc_crud/proto/crud"
	"log"
	"net"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
)

const (
	port    = ":50051"
	dbHost  = "localhost"
	dbPort  = "3306"
	dbUser  = "root"
	dbPass  = ""
	dbName  = "db_skripsi_2"
)

type server struct {
	db *sql.DB
	crud.UnimplementedCrudServiceServer // Embed the UnimplementedCrudServiceServer
}

func (s *server) ReadAll(ctx context.Context, req *crud.ReadAllRequest) (*crud.ReadAllResponse, error) {
	
	rows, err := s.db.Query("SELECT b.nama_barang, b.foto_barang, b.harga, k.nama_kategori, j.nama_jenis, rb.no_batch FROM ref_barang rb INNER JOIN barang b ON rb.id_barang = b.id_barang INNER JOIN kategori k ON b.id_kategori = k.id_kategori INNER JOIN material m ON b.id_material = m.id_material INNER JOIN jenis j ON b.id_jenis = j.id_jenis")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var responses []*crud.ResponseRead
	for rows.Next() {
		var namaBarang, fotoBarang, namaKategori, namaJenis, noBatch string
		var harga string
	
		err := rows.Scan(&namaBarang, &fotoBarang, &harga, &namaKategori, &namaJenis, &noBatch)
		if err != nil {
			return nil, err
		}

		hargaInt, err := strconv.Atoi(harga)
		if err != nil {
			return nil, err
		}
		
		response := &crud.ResponseRead{
			NamaBarang:    namaBarang,
			FotoBarang:    fotoBarang,
			Harga:         int32(hargaInt),
			NamaKategori:  namaKategori,
			NamaJenis:     namaJenis,
			NoBatch:       noBatch,
		}
		responses = append(responses, response)
	}

	
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &crud.ReadAllResponse{Responses: responses}, nil
}

func (s *server) ReadWithCategory(ctx context.Context, req *crud.ReadWithCategoryRequest) (*crud.ReadWithCategoryResponse, error) {
	
	rows, err := s.db.Query("SELECT b.nama_barang, b.foto_barang, k.nama_kategori, b.harga FROM barang b INNER JOIN kategori k on b.id_kategori = k.id_kategori")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	
	var responses []*crud.ResponseReadCategory
	for rows.Next() {
		var namaBarang, fotoBarang, namaKategori string
		var harga string
		err := rows.Scan(&namaBarang, &fotoBarang, &namaKategori, &harga)
		if err != nil {
			return nil, err
		}

		hargaInt, err := strconv.Atoi(harga)
		if err != nil {
			return nil, err
		}

		response := &crud.ResponseReadCategory{
			NamaBarang:    namaBarang,
			FotoBarang:    fotoBarang,
			Harga:         int32(hargaInt),
			NamaKategori:  namaKategori,
		}
		responses = append(responses, response)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &crud.ReadWithCategoryResponse{Responses: responses}, nil
}

func (s *server) ReadWithJenis(ctx context.Context, req *crud.ReadWithJenisRequest) (*crud.ReadWithJenisResponse, error) {
	
	rows, err := s.db.Query("SELECT b.nama_barang, b.foto_barang, k.nama_jenis, b.harga FROM barang b INNER JOIN jenis k on b.id_jenis = k.id_jenis")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	
	var responses []*crud.ResponseReadJenis
	for rows.Next() {
		var namaBarang, fotoBarang, namaJenis string
		var harga string
		err := rows.Scan(&namaBarang, &fotoBarang, &namaJenis, &harga)
		if err != nil {
			return nil, err
		}

		hargaInt, err := strconv.Atoi(harga)
		if err != nil {
			return nil, err
		}

		response := &crud.ResponseReadJenis{
			NamaBarang:    namaBarang,
			FotoBarang:    fotoBarang,
			Harga:         int32(hargaInt),
			NamaJenis:  namaJenis,
		}
		responses = append(responses, response)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &crud.ReadWithJenisResponse{Responses: responses}, nil
}

func (s *server) ReadWithMaterial(ctx context.Context, req *crud.ReadWithMaterialRequest) (*crud.ReadWithMaterialResponse, error) {
	
	rows, err := s.db.Query("SELECT b.nama_barang, b.foto_barang, k.nama_material, b.harga FROM barang b INNER JOIN material k on b.id_material = k.id_material")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var responses []*crud.ResponseReadMaterial
	for rows.Next() {
		var namaBarang, fotoBarang, namaMaterial string
		var harga string
		
		err := rows.Scan(&namaBarang, &fotoBarang, &namaMaterial, &harga)
		if err != nil {
			return nil, err
		}

		hargaInt, err := strconv.Atoi(harga)
		if err != nil {
			return nil, err
		}

		response := &crud.ResponseReadMaterial{
			NamaBarang:    namaBarang,
			FotoBarang:    fotoBarang,
			Harga:         int32(hargaInt),
			NamaMaterial: namaMaterial,
		}
		responses = append(responses, response)
	}

	
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &crud.ReadWithMaterialResponse{Responses: responses}, nil
}

func (s *server) ReadWithBatch(ctx context.Context, req *crud.ReadWithBatchRequest) (*crud.ReadWithBatchResponse, error) {

	rows, err := s.db.Query("SELECT b.nama_barang, b.foto_barang, k.no_batch, b.harga FROM barang b INNER JOIN ref_barang k on b.id_barang = k.id_barang")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var responses []*crud.ResponseReadBatch
	for rows.Next() {
		var namaBarang, fotoBarang, NoBatch string
		var harga string
		err := rows.Scan(&namaBarang, &fotoBarang, &NoBatch, &harga)
		if err != nil {
			return nil, err
		}

		hargaInt, err := strconv.Atoi(harga)
		if err != nil {
			return nil, err
		}
		response := &crud.ResponseReadBatch{
			NamaBarang:    namaBarang,
			FotoBarang:    fotoBarang,
			Harga:         int32(hargaInt),
			NomorBatch: NoBatch,
		}
		responses = append(responses, response)
	}


	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &crud.ReadWithBatchResponse{Responses: responses}, nil
}

func (s *server) ReadExpiredBarang(ctx context.Context, req *crud.ReadExpiredBarangRequest) (*crud.ReadExpiredBarangResponse, error) {
	
	rows, err := s.db.Query("SELECT b.nama_barang, rb.stok, rb.no_batch, rb.expired FROM ref_barang rb INNER JOIN barang b ON rb.id_barang = b.id_barang  WHERE rb.expired <= CURRENT_DATE")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	
	var responses []*crud.ResponseReadExpired

	for rows.Next() {
		var namaBarang, stok, NoBatch, tglExpired string
	
		err := rows.Scan(&namaBarang, &stok, &NoBatch, &tglExpired)
		if err != nil {
			return nil, err
		}

		stokInt, err := strconv.Atoi(stok)
		if err != nil {
			return nil, err
		}

		response := &crud.ResponseReadExpired{
			NamaBarang:    namaBarang,
			NomorBatch: NoBatch,
			Stok:    int32(stokInt),
			TglExpired: tglExpired,
		}
		responses = append(responses, response)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &crud.ReadExpiredBarangResponse{Responses: responses}, nil
}

func (s *server) ReadNotExpiredBarang(ctx context.Context, req *crud.ReadNotExpiredBarangRequest) (*crud.ReadNotExpiredBarangResponse, error) {

	rows, err := s.db.Query("SELECT b.nama_barang, rb.stok, rb.no_batch, rb.expired FROM ref_barang rb INNER JOIN barang b ON rb.id_barang = b.id_barang  WHERE rb.expired >= CURRENT_DATE")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var responses []*crud.ResponseReadNotExpired

	for rows.Next() {
		var namaBarang, stok, NoBatch, tglExpired string
		
		err := rows.Scan(&namaBarang, &stok, &NoBatch, &tglExpired)
		if err != nil {
			return nil, err
		}

		
		stokInt, err := strconv.Atoi(stok)
		if err != nil {
			return nil, err
		}

	
		response := &crud.ResponseReadNotExpired{
			NamaBarang:    namaBarang,
			NomorBatch: NoBatch,
			Stok:    int32(stokInt),
			TglExpired: tglExpired,
		}
		responses = append(responses, response)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &crud.ReadNotExpiredBarangResponse{Responses: responses}, nil
}

func (s *server) UpdateHargaBatch(ctx context.Context, req *crud.UpdateHargaBatchRequest) (*crud.UpdateHargaBatchResponse, error) {
	
	_, err := s.db.Exec("UPDATE barang b INNER JOIN ref_barang rb ON rb.id_barang = b.id_barang SET b.harga =? WHERE rb.no_batch =?", req.Harga,req.NomorBatch)
    if err != nil {
        return nil, err
    }

	return &crud.UpdateHargaBatchResponse{Success: true, Message: "Data updated successfully"}, nil
}

func (s *server) CreateBulkRef(ctx context.Context, req *crud.CreateBulkRefRequest) (*crud.CreateBulkRefResponse, error) {
  
    query := "INSERT INTO `ref_barang` (`id_ref_barang`, `id_barang`, `stok`, `expired`, `no_batch`, `created_date`) VALUES (NULL,?,?,?,?, current_timestamp());"

    tx, err := s.db.Begin()
    if err != nil {
        return nil, err
    }
    defer func() {
        if err != nil {
           
            tx.Rollback()
            return
        }
        tx.Commit()
    }()

    stmt, err := tx.Prepare(query)
    if err != nil {
        return nil, err
    }
    defer stmt.Close()

    for _, data := range req.Data {

        _, err := stmt.Exec(data.IdBarang, data.Stok, data.ExpDate, data.NoBatch)
        if err != nil {
            return nil, err
        }
    }

   
    return &crud.CreateBulkRefResponse{
        Success: true,
        Message: "Bulk create successful",
    }, nil
}


func main() {
	listen, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	fmt.Printf("Server listening on port %s\n", port)

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPass, dbHost, dbPort, dbName))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	s := grpc.NewServer()
	crud.RegisterCrudServiceServer(s, &server{db: db})

	if err := s.Serve(listen); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
