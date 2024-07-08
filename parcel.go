package main

import (
	"database/sql"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	queryText := "INSERT INTO parcel (client, status, address, created_at) VALUES (:client, :status, :address, :created_at)"

	res, err := s.db.Exec(queryText, sql.Named("client", p.Client),
		sql.Named("status", p.Status), sql.Named("address", p.Address), sql.Named("created_at", p.CreatedAt))

	if err != nil {
		return -1, err
	}

	lastInd, err := res.LastInsertId()
	if err != nil {
		return -1, err
	}
	// верните идентификатор последней добавленной записи
	return int(lastInd), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	queryText := "SELECT client, status, address, created_at, number FROM parcel WHERE number = :parcelNumber"

	res := s.db.QueryRow(queryText, sql.Named("parcelNumber", number))

	// заполните объект Parcel данными из таблицы
	p := Parcel{}
	err := res.Scan(&p.Client, &p.Status, &p.Address, &p.CreatedAt, &p.Number)
	if err != nil {
		return p, err
	}
	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	queryText := "SELECT number, client, status, address, created_at FROM parcel WHERE client = :clientId"
	var res []Parcel
	rows, err := s.db.Query(queryText, sql.Named("clientId", client))
	if err != nil {
		return res, err
	}
	// заполните срез Parcel данными из таблицы

	for rows.Next() {
		p := Parcel{}
		err = rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		if err != nil {
			return nil, err
		}
		res = append(res, p)
	}

	defer rows.Close()

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {

	queryText := "UPDATE parcel SET status = :status WHERE number = :number"
	_, err := s.db.Exec(queryText, sql.Named("status", status), sql.Named("number", number))
	// реализуйте обновление статуса в таблице parcel

	return err
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	p, err := s.Get(number)
	if err != nil {
		return err
	}

	pushNil := p.Status != ParcelStatusRegistered
	if pushNil {
		return nil
	}

	queryText := "UPDATE parcel SET address = :address WHERE number = :number"
	_, err = s.db.Exec(queryText, sql.Named("address", address), sql.Named("number", number))

	return err
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered

	p, err := s.Get(number)
	if err != nil {
		return err
	}

	pushNil := p.Status != ParcelStatusRegistered
	if pushNil {
		return nil
	}

	queryText := "DELETE FROM parcel WHERE number = :number"
	_, err = s.db.Exec(queryText, sql.Named("number", number))

	return err
}
